"""
Copernicus Data Space Ecosystem (CDSE) authentication helpers.
"""

import os
import time
import stat
from pathlib import Path
import requests
import threading
import logging
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from typing import Dict


AUTH_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

def _now() -> float: 
    """
    Get the current time as a float (seconds since epoch).
    """
    return time.time()


def _access_token_grant(credentials: dict) -> dict:
    """
    Request an access token using the Resource Owner Password Credentials grant.
    """
    data = {
        "client_id": "cdse-public",
        "grant_type": "password",
        "username": credentials["username"],
        "password": credentials["password"],
    }
    response = requests.post(AUTH_URL, data=data, timeout=30)
    response.raise_for_status()
    return response.json()


def _refresh_grant(refresh_token: str) -> dict:
    """
    Refresh the access token using the refresh token.
    """
    data = {
        "client_id": "cdse-public",
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    response = requests.post(AUTH_URL, data=data, timeout=30)
    response.raise_for_status()
    return response.json()


def _read_password_file(path: str | None) -> str | None:
    """
    Read password from file, ensuring secure permissions.
    """

    # get metadata about the password file
    if not path:
        return None
    p = Path(path).expanduser()  # get full path
    metadata = p.stat()
    
    # don't read the password file if it has insecure permissions
    if metadata.st_mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise RuntimeError(f"Insecure permissions on {p}; run: chmod 600 {p}")
    
    # otherwise, read and return the password
    return p.read_text(encoding="utf-8").strip()


def _fill_creds(creds: dict) -> dict:
    """
    Create a copy of creds dict, filling in missing username/password if needed.
    """
    # work on a copy to avoid surprising callers
    out = dict(creds)

    # fill from env
    if not out.get("username"):
        env_user = os.getenv("CDSE_USERNAME")
        if env_user:
            out["username"] = env_user

    # fill from env or file
    if not out.get("password"):
        pw = os.getenv("CDSE_PASSWORD")
        if not pw:
            pw_file = os.getenv("CDSE_PASSWORD_FILE")
            if pw_file:
                pw = _read_password_file(pw_file)
        if pw:
            out["password"] = pw

    return out


def get_access_token(
    credentials: dict,
    token_cache: dict,
    *,
    force_refresh: bool = False,
) -> str:
    """
    Get a valid access token, refreshing for credentials as needed.
    """
    now = _now()
    creds = dict(credentials)

    # check existing token
    tok = token_cache.get("access_token")
    exp = float(token_cache.get("expires_at") or 0)
    if tok and (exp > now + 60) and not force_refresh:
        return tok

    # try refresh token first
    rtok = token_cache.get("refresh_token")
    rtexp = float(token_cache.get("refresh_expires_at") or 0)

    # if refresh token is valid, try to use it
    if rtok and (rtexp > now + 60):
        try:
            j = _refresh_grant(rtok)
            token_cache["access_token"] = j["access_token"]
            token_cache["expires_at"] = now + int(j.get("expires_in", 3600))
            if "refresh_token" in j:
                token_cache["refresh_token"] = j["refresh_token"]
                token_cache["refresh_expires_at"] = now + int(j.get("refresh_expires_in", 0) or 0)
            return token_cache["access_token"]
        except Exception:
            pass

    # otherwise, do password grant
    creds = _fill_creds(creds)
    if "username" not in creds or "password" not in creds:
        raise RuntimeError(
            "CDSE credentials are required.\n"
            "Provide them using one of:\n"
            " a) CDSE_USERNAME and CDSE_PASSWORD environment variables\n"
            " b) CDSE_PASSWORD_FILE pointing to a chmod 600 file containing the pass\n"
        )

    # finally, get the access token grant
    j = _access_token_grant(creds)
    token_cache["access_token"] = j["access_token"]
    token_cache["expires_at"] = now + int(j.get("expires_in", 3600))
    token_cache["refresh_token"] = j.get("refresh_token")
    token_cache["refresh_expires_at"] = now + int(j.get("refresh_expires_in", 0) or 0)
    return token_cache["access_token"]


class AutoRefreshSession(requests.Session):
    """requests.Session with CDSE bearer auth and automatic token refresh."""

    _refresh_lock = threading.Lock()

    def __init__(self, credentials: dict, token_cache: dict, logger: logging.Logger | None = None):
        super().__init__()
        self._credentials = credentials
        self._token_cache = token_cache
        self._logger = logger

        tok = get_access_token(self._credentials, self._token_cache, force_refresh=False)
        self.headers.update({"Authorization": f"Bearer {tok}"})

        retries = Retry(
            total=5,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST"]),
        )
        adapter = HTTPAdapter(pool_connections=2, pool_maxsize=2, max_retries=retries)
        self.mount("https://", adapter)
        self.mount("http://", adapter)

    def request(self, method, url, **kwargs):
        resp = super().request(method, url, **kwargs)
        if resp.status_code == 401:
            if self._logger:
                self._logger.info("token expired; refreshing and retrying once...")
            with AutoRefreshSession._refresh_lock:
                new_tok = get_access_token(self._credentials, self._token_cache, force_refresh=True)
                self.headers["Authorization"] = f"Bearer {new_tok}"
            resp = super().request(method, url, **kwargs)
        return resp


def make_auto_session(
    credentials: dict,
    token_cache: dict,
    logger: logging.Logger | None = None,
) -> AutoRefreshSession:
    """Factory for AutoRefreshSession; keeps auth details in one place."""
    return AutoRefreshSession(credentials, token_cache, logger=logger)