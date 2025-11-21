"""
Functions to support Sentinel-2 Surface Reflectance masking.
"""

from pathlib import Path
import re
from typing import List, Optional, Set, Tuple
import numpy as np
import pandas as pd
from logging import Logger
from osgeo import gdal
import textwrap
from lxml import etree
import copy
import datetime as dt

from sentinel_py.common.gdal import add_python_pixelfunc_to_vrt
from sentinel_py.common.utils import extract_s2_acq_date, in_season_window

S2_RES_OPTS = [10, 20, 60]  # available Sentinel-2 resolutions in meters
PB_OFFSET_CODE = textwrap.dedent(
    """
    import numpy as np

    def pb_offset(in_ar, out_ar, *args, **kwargs):
        dn_off = int(kwargs.get("dn_offset", 0))
        nd = int(kwargs.get("nodata", 65535))

        # Single input source
        A = in_ar[0].astype(np.uint16, copy=False)
        out = A.copy()

        valid = (out != nd)

        if dn_off != 0 and valid.any():
            tmp = out.astype(np.int32, copy=True)
            np.subtract(tmp, dn_off, out=tmp, where=valid)
            np.clip(tmp, 0, 65534, out=tmp)
            out[valid] = tmp[valid].astype(np.uint16)

        out_ar[:] = out
    """
)
MASK_CODE = textwrap.dedent(
    """
    import numpy as np, re
    LUT = None

    def _to_int(x, default=0):
        if x is None:
            return default
        if isinstance(x, bytes):
            x = x.decode('utf-8', 'ignore')
        s = str(x).strip().strip('"').strip("'")
        return default if s == "" else int(s)

    def _to_csv_str(x):
        if x is None:
            return ""
        if isinstance(x, bytes):
            x = x.decode('utf-8', 'ignore')
        return str(x).strip().strip('"').strip("'")

    def scl_mask(in_ar, out_ar, *args, **kwargs):
        # in_ar[0] = PB-offset band
        # in_ar[1] = SCL
        A = in_ar[0].astype(np.uint16, copy=False)
        B = in_ar[1].astype(np.uint8,  copy=False)

        nd = _to_int(kwargs.get("nodata"), 65535)
        classes = _to_csv_str(kwargs.get("classes"))

        global LUT
        if LUT is None:
            codes = [int(t) for t in re.findall(r"\\d+", classes)] if classes else []
            LUT = np.zeros(256, dtype=bool)
            if codes:
                LUT[codes] = True

        # Always mask SCL==0 and any code > 11
        mask = (B == 0) | (B > 11)
        if LUT is not None:
            mask |= LUT[B]

        out = A.copy()
        out[mask] = nd
        out_ar[:] = out
    """
)

def get_band_paths(
    data_dir: Path,
    bands: List[str],
    target_res_m: int,
    years: Optional[Set[int]] = None,
    period_start: Optional[Tuple[int, int]] = None,  # (month, day)
    period_end: Optional[Tuple[int, int]] = None,    # (month, day)
    logger: Logger | None = None,
) -> pd.DataFrame:
    """
    Scan a directory tree for Sentinel-2 Surface Reflectance .jp2 files and
    return a DataFrame of band paths filtered by acquisition date.

    Parameters
    ----------
    data_dir : Path
        Root directory containing Sentinel-2 L2A products (.SAFE directories).
    bands : list of str
        Band IDs to look for, e.g. ["B02", "B03", "B04"].
    target_res_m : int
        Desired resolution (10, 20, or 60). If no files are found at this
        resolution for a given band, we fall back to other resolutions in
        S2_RES_OPTS (in order).
    years : set of int, optional
        Only keep scenes whose acquisition year is in this set. If None,
        no year filtering is applied.
    period_start, period_end : (month, day), optional
        Seasonal window as integer (MM, DD) tuples. If both are provided, only keep
        scenes where in_season_window(acq_date, period_start, period_end)
        is True.
    logger : logging.Logger, optional
        Logger for warnings/info.

    Returns
    -------
    DataFrame
        Columns:
          - "band": band ID (e.g. "B02")
          - "band_jp2_path": Path to the JP2 file
          - "acq_date": datetime.date
          - "resolution_m": int (actual resolution used)
    """
    records: list[dict] = []

    for band in bands:
        # Check if target resolution file exists
        pattern = f"**/R{target_res_m}m/*_{band}_{target_res_m}m.jp2"
        matches = list(data_dir.glob(pattern))
        resolutions_to_try: List[int]
        if matches:
            resolutions_to_try = [target_res_m]
        else:
            if logger:
                logger.warning(
                    f"No files found for band {band} at {target_res_m}m in {data_dir}"
                )
            resolutions_to_try = [r for r in S2_RES_OPTS if r != target_res_m]

        # No files at target resolution for this band: try fallbacks
        found_any = False
        for res in resolutions_to_try:
            pattern = f"**/R{res}m/*_{band}_{res}m.jp2"
            res_matches = list(data_dir.glob(pattern))
            if not res_matches:
                continue

            # Fallback resolution found
            if res != target_res_m and logger:
                logger.info(
                    f"Found band {band} at {res}m instead of {target_res_m}m"
                )
            found_any = True
            for jp2_path in res_matches:
                # Extract acquisition date
                acq_date = extract_s2_acq_date(jp2_path)
                if acq_date is None:
                    if logger:
                        logger.warning(f"Could not extract acq_date from {jp2_path}")
                    continue

                # Ignore scenes not in requested years
                if years and acq_date.year not in years:
                    continue

                # Ignore scenes not in seasonal window
                if period_start and period_end:
                    if not in_season_window(acq_date, period_start, period_end):
                        continue

                # Build record (row and cols)
                records.append(
                    {
                        "band": band,
                        "band_jp2_path": jp2_path,
                        "acq_date": acq_date,
                        "resolution_m": res,
                    }
                )

            # If we found matches at target resolution, we do NOT look at fallback res
            if res == target_res_m:
                break

        # If no files found at any resolution for this band, raise error
        if not found_any:
            msg = (
                f"No files found for band {band} at any of "
                f"{S2_RES_OPTS} m in {data_dir}"
            )
            if logger:
                logger.error(msg)
            raise RuntimeError(msg)

    # No scenes matched the date filters; return an empty but typed DataFrame
    if not records:
        return pd.DataFrame(
            columns=["band", "band_jp2_path", "acq_date", "resolution_m"]
        )

    df = pd.DataFrame.from_records(records)
    return df


def get_scl_mask_paths(
    band_jp2_path: Path,
    logger: Logger | None = None,
) -> Optional[Path]:
    """
    Given the path to a Sentinel-2 Surface Reflectance .jp2 file, return the path
    to the corresponding Scene Classification Layer (SCL) mask file, or None if not found.
    """
    if not band_jp2_path.exists():
        raise FileNotFoundError(f"Band file not found: {band_jp2_path}")

    band_filename = band_jp2_path.name
    scl_filename = band_filename.replace("_B", "_SCL")

    # 1) Same resolution folder first
    scl_jp2_path = band_jp2_path.parent / scl_filename
    if scl_jp2_path.exists():
        return scl_jp2_path

    if logger:
        logger.info(f"SCL file not found at expected location: {scl_jp2_path}")

    # We expect something like .../IMG_DATA/R20m/...
    current_res_dir = band_jp2_path.parent.name  # e.g. "R20m"

    # 2) Try other resolution folders under IMG_DATA
    img_data_dir = band_jp2_path.parent.parent  # e.g. .../IMG_DATA
    for res_opt in S2_RES_OPTS:
        res_dir_name = f"R{res_opt}m"
        if res_dir_name == current_res_dir:
            continue
        alt_scl_jp2_path = img_data_dir / res_dir_name / scl_filename
        if alt_scl_jp2_path.exists():
            if logger:
                logger.info(
                    f"Using SCL file found at {res_opt}m resolution: {alt_scl_jp2_path}"
                )
            return alt_scl_jp2_path

    if logger:
        logger.warning(f"No SCL found for: {band_jp2_path}")
    return None


def get_pb_offset_from_jp2(
    band_jp2_path: Path,
    logger: Logger | None = None,
) -> int:
    """
    Infer the DN offset from the Sentinel-2 processing baseline (PB)
    encoded in the .SAFE directory name of the given .jp2 file.

    Returns
    -------
    int
        1000 if PB >= 4.00, otherwise 0.
    """
    band_jp2_path = Path(band_jp2_path)

    # Walk up until we find the .SAFE directory
    safe_dir: Optional[Path] = None
    for parent in band_jp2_path.parents:
        if parent.name.endswith(".SAFE"):
            safe_dir = parent
            break

    if safe_dir is None:
        if logger:
            logger.error(f"Could not locate .SAFE directory above: {band_jp2_path}")
        return 0

    # Extract PB value from SAFE name, e.g. "..._N0500_..."
    match = re.search(r"_N(\d{4})_", safe_dir.name)
    if not match:
        if logger:
            logger.error(f"Could not extract PB value from SAFE name: {safe_dir.name}")
        return 0

    pb = float(match.group(1)) / 100.0  # N0500 -> 5.00, N0400 -> 4.00
    dn_offset = 1000 if pb >= 4.00 else 0
    return dn_offset


def create_pb_offset_vrt(
    band_jp2_path: Path,
    dn_offset: int,
    out_vrt_dir: Path | None = None,
    *,
    dst_nodata: int = 65535,
    logger: Logger | None = None,
) -> Path:
    """
    Create a VRT that applies a PB-offset correction to a Sentinel-2 band.
    The output is always placed in a directory (not a file path), and the
    filename is automatically derived from the JP2 basename.

    Output name: <basename>.pb_offset.vrt
    Example:
        T06WVB_20200616T213531_B03_20m.jp2
        → <out_vrt_dir>/T06WVB_20200616T213531_B03_20m.pb_offset.vrt

    Parameters
    ----------
    band_jp2_path : Path
        Path to the original Sentinel-2 reflectance JP2 band.
    dn_offset : int
        PB offset (0 or 1000 for S2 L2A).
    out_vrt_dir : Path or None
        Directory in which VRTs will be written. If None, defaults to
        ~/.sentinel-py/temp/.
    dst_nodata : int
        Nodata value injected when subtracting offset.
    logger : Optional[Logger]
        Logger for diagnostics.

    Returns
    -------
    Path
        Path to the created VRT file.
    """
    band_jp2_path = Path(band_jp2_path)

    # Determine output VRT directory
    if out_vrt_dir is None:
        out_vrt_dir = (
            Path.home() / ".sentinel-py" / "temp"
        )
        if logger:
            logger.warning(f"No out_vrt_dir provided; using default: {out_vrt_dir}")
    out_vrt_dir = Path(out_vrt_dir)
    out_vrt_dir.mkdir(parents=True, exist_ok=True)

    # Construct output file path inside the directory
    out_vrt_file = out_vrt_dir / band_jp2_path.with_suffix(".pb_offset.vrt").name

    # Build passthrough VRT
    gdal.Translate(str(out_vrt_file), str(band_jp2_path), format="VRT")

    # No offset is needed, so just return the passthrough VRT
    if dn_offset == 0:
        if logger:
            logger.info(
                f"No PB offset needed for {band_jp2_path}. "
                f"VRT created at {out_vrt_file}"
            )
        return out_vrt_file

    # Add Python pixel function to VRT to apply the offset
    add_python_pixelfunc_to_vrt(
        out_vrt_file,
        func_name="pb_offset",
        func_code=PB_OFFSET_CODE,
        args={"dn_offset": str(dn_offset), "nodata": str(dst_nodata)},
    )
    if logger:
        logger.info(
            f"Applied PB offset={dn_offset} to {band_jp2_path}. "
            f"VRT written to {out_vrt_file}"
        )

    return out_vrt_file


def create_masked_vrt(
    band_vrt_path: Path,
    scl_jp2_path: Path,
    masking_scl_values: List[int],
    out_vrt_path: Path | None = None,
    *,
    dst_nodata: int = 65535,
    logger: Logger | None = None,
) -> Path:
    """
    Create a VRT that masks a PB-offset band using the SCL band via a Python pixel function.

    Parameters
    ----------
    band_vrt_path : Path
        Path to the PB-offset VRT (single-band).
    scl_jp2_path : Path
        Path to the Sentinel-2 SCL JP2.
    masking_scl_values : list[int]
        SCL codes to mask in addition to the default (0 and >11).
    out_vrt_path : Path, optional
        Output VRT path. If None, derive from band_vrt_path.
    dst_nodata : int, optional
        Nodata value to write into masked pixels.

    Returns
    -------
    Path
        Path to the masked VRT.
    """
    # ensure paths are Path objects
    band_vrt_path = Path(band_vrt_path)
    scl_jp2_path = Path(scl_jp2_path)

    # set up output VRT path
    if out_vrt_path is None:
        # e.g. "B02.pb_offset.vrt" -> "B02.pb_offset.masked.vrt"
        out_vrt_path = band_vrt_path.with_suffix(band_vrt_path.suffix + ".masked.vrt")
        logger.warning(f"No out_vrt_path provided. Using default: {out_vrt_path}")
    out_vrt_path = Path(out_vrt_path)
    out_vrt_path.parent.mkdir(parents=True, exist_ok=True)

    # build a temporary VRT with two separate bands:
    # band 1 -> PB-offset band (band_vrt_path)
    # band 2 -> SCL JP2 (scl_jp2_path)
    tmp_vrt = out_vrt_path.with_suffix(out_vrt_path.suffix + ".tmp")
    tmp_vrt = tmp_vrt.resolve()  # ensure full path
    tmp_vrt.unlink(missing_ok=True)

    gdal.BuildVRT(
        str(tmp_vrt),
        [str(band_vrt_path), str(scl_jp2_path)],
        separate=True,
    )

    # edit the VRT XML:
    # - turn band 1 into a VRTDerivedRasterBand with a Python pixel function
    # - make band 1 reference *both* sources (pb band + SCL)
    # - drop band 2 from the final dataset
    tree = etree.parse(str(tmp_vrt))
    root = tree.getroot()

    band1 = root.find(".//VRTRasterBand[@band='1']")
    band2 = root.find(".//VRTRasterBand[@band='2']")
    if band1 is None or band2 is None:
        logger.error(f"Expected 2 bands in {tmp_vrt}, found less.")
        raise RuntimeError(f"Expected 2 bands in {tmp_vrt}, found less.")

    # copy SimpleSource(s) from band2 into band1
    sources2 = band2.findall("SimpleSource")
    if not sources2:
        logger.error(f"No SimpleSource elements in band 2 of {tmp_vrt}")
        raise RuntimeError(f"No SimpleSource elements in band 2 of {tmp_vrt}")

    for src in sources2:
        band1.append(copy.deepcopy(src))

    # remove band2 from the dataset so we end with a single-band VRT
    band2_parent = band2.getparent()
    band2_parent.remove(band2)

    # make band1 a derived band
    band1.set("subClass", "VRTDerivedRasterBand")

    # ensure NoDataValue element is set correctly
    nd_elem = band1.find("NoDataValue")
    if nd_elem is None:
        nd_elem = etree.SubElement(band1, "NoDataValue")
    nd_elem.text = str(dst_nodata)

    # remove any existing pixel-function elements (if present)
    for tag in (
        "PixelFunctionLanguage",
        "PixelFunctionType",
        "PixelFunctionArguments",
        "PixelFunctionCode",
    ):
        for el in band1.findall(tag):
            band1.remove(el)

    # add new pixel-function elements
    lang_el = etree.SubElement(band1, "PixelFunctionLanguage")
    lang_el.text = "Python"

    type_el = etree.SubElement(band1, "PixelFunctionType")
    type_el.text = "scl_mask"

    args_el = etree.SubElement(band1, "PixelFunctionArguments")
    classes_csv = ",".join(map(str, sorted(set(masking_scl_values))))
    args_el.set("nodata", str(dst_nodata))
    args_el.set("classes", classes_csv)

    code_el = etree.SubElement(band1, "PixelFunctionCode")
    code_el.text = etree.CDATA(MASK_CODE)

    # write the final VRT
    tree.write(str(out_vrt_path), pretty_print=True, xml_declaration=True, encoding="UTF-8")
    tmp_vrt.unlink(missing_ok=True)
    return out_vrt_path