# sentinel_py/common/cdse_types.py

from enum import Enum


# General CVs --------------------------------------------------------------------------
class CDSECollections(str, Enum):
    """CDSE collection names that can be downloaded with this CLI."""

    sentinel2 = "SENTINEL-2"
    sentinel1 = "SENTINEL-1"


class CDSEOrbitDirs(str, Enum):
    """CDSE orbit direction values that can be used to filter queries with this CLI."""

    ascending = "ASCENDING"
    descending = "DESCENDING"


# Sentinel-1 CVs -----------------------------------------------------------------------
class S1Products(str, Enum):
    """CDSE Sentinel-1 product types that can be downloaded with this CLI."""

    grd = "GRD"


class S1SerialIds(str, Enum):
    """CDSE Sentinel-1 platform serial identifiers that can be used to filter queries with this CLI."""

    sentinel1a = "A"
    sentinel1b = "B"
    sentinel1c = "C"


class S1Swaths(str, Enum):
    """CDSE Sentinel-1 swath identifiers that can be used to filter queries with this CLI."""

    s1 = "S1"
    s2 = "S2"
    s3 = "S3"
    s4 = "S4"
    s5 = "S5"
    s6 = "S6"
    iw1 = "IW1"
    iw2 = "IW2"
    iw3 = "IW3"
    ew1 = "EW1"
    ew2 = "EW2"
    ew3 = "EW3"
    ew4 = "EW4"
    ew5 = "EW5"
    wv1 = "WV1"
    wv2 = "WV2"


class S1SensorModes(str, Enum):
    """CDSE Sentinel-1 sensor modes that can be used to filter queries with this CLI."""

    stripmap = "SM"  # 80 km swath, 5 m resolution, 6 beams (S1-S6)
    interferometric_wide_swath = "IW"  # 250 km swath, 5×20 m resolution, 3 sub-swaths
    extra_wide_swath = "EW"  # 400 km swath, 20×40 m resolution, 5 sub-swaths
    wave = "WV"  # 20×20 km vignettes, 5 m resolution, for ocean applications


class S1Bands(str, Enum):
    """CDSE Sentinel-1 bands that can be downloaded with this CLI."""

    vv = "VV"
    vh = "VH"
    hh = "HH"
    hv = "HV"

    @classmethod
    def default_bands(cls) -> list["S1Bands"]:
        return [cls.vv, cls.vh]


class S1Pols(str, Enum):
    """CDSE Sentinel-1 polarization modes that can be used to filter queries with this CLI."""

    hh = "HH"
    vv = "VV"
    hhvh = "HH&VH"
    vvvh = "VV&VH"
    vhvv = "VH&VV"
    vhhh = "VH&HH"
    hhhv = "HH&HV"
    vvhv = "VV&HV"
    hvhh = "HV&HH"
    hvvv = "HV&VV"


# Sentinel-2 CVs -----------------------------------------------------------------------
class S2Products(str, Enum):
    """CDSE Sentinel-2 product types that can be downloaded with this CLI."""

    msi1c = "S2MSI1C"
    msi2a = "S2MSI2A"
    msi2b = "S2MSI2B"


class S2SerialIds(str, Enum):
    """CDSE Sentinel-2 platform serial identifiers that can be used to filter queries with this CLI."""

    sentinel2a = "A"
    sentinel2b = "B"


class S2SensorModes(str, Enum):
    """CDSE Sentinel-2 sensor modes that can be used to filter queries with this CLI."""

    normal = "INS-NOBS"
    raw = "INS-RAW"
    vicarious = "INS-VIC"


class S2Res(str, Enum):
    r10m = "10"
    r20m = "20"
    r60m = "60"


class S2Bands(str, Enum):
    """CDSE Sentinel-2 bands that can be downloaded with this CLI."""

    b02 = "B02"
    b03 = "B03"
    b04 = "B04"
    b05 = "B05"
    b06 = "B06"
    b07 = "B07"
    b08 = "B08"
    b8a = "B8A"
    b09 = "B09"
    b10 = "B10"
    b11 = "B11"
    b12 = "B12"

    @classmethod
    def default_bands(cls) -> list["S2Bands"]:
        """Default bands to download if not specified in `download` command."""
        return [
            cls.b02,
            cls.b03,
            cls.b04,
            cls.b05,
            cls.b06,
            cls.b07,
            cls.b08,
            cls.b8a,
            cls.b11,
            cls.b12,
        ]


# Collection-specific helpers ----------------------------------------------------------

COLLECTION_BANDS = {
    CDSECollections.sentinel2: S2Bands,
    CDSECollections.sentinel1: S1Bands,
}

COLLECTION_DEFAULTS = {
    CDSECollections.sentinel2: S2Bands.default_bands(),
    CDSECollections.sentinel1: S1Bands.default_bands(),
}


def validate_bands(collection: CDSECollections, bands: list[str]) -> list[str]:
    """Validate bands against the collection and return normalized values."""
    band_enum = COLLECTION_BANDS.get(collection)
    if band_enum is None:
        return bands

    valid = {b.value for b in band_enum}
    normalized = [b.upper() for b in bands]
    invalid = [b for b in normalized if b not in valid]
    if invalid:
        raise ValueError(
            f"Invalid bands for {collection.value}: {invalid}. Valid: {sorted(valid)}"
        )
    return normalized


def validate_product(collection: CDSECollections, product: str) -> str:
    """Validate product against the collection and return normalized value."""
    if not product:
        return product
    if collection == CDSECollections.sentinel2:
        valid = {p.value for p in S2Products}
    elif collection == CDSECollections.sentinel1:
        valid = {p.value for p in S1Products}
    else:
        raise ValueError(
            f"Unsupported collection for product validation: {collection.value}"
        )

    normalized = product.upper()
    if normalized not in valid:
        raise ValueError(
            f"Invalid product for {collection.value}: {normalized}. "
            f"Valid options: {sorted(valid)}"
        )
    return normalized


def validate_serial_id(collection: CDSECollections, serial_id: str) -> str:
    """Validate platform serial ID against the collection and return normalized value."""
    if not serial_id:
        return serial_id
    if collection == CDSECollections.sentinel2:
        valid = {s.value for s in S2SerialIds}
    elif collection == CDSECollections.sentinel1:
        valid = {s.value for s in S1SerialIds}
    else:
        raise ValueError(
            f"Unsupported collection for serial ID validation: {collection.value}"
        )

    normalized = serial_id.upper()
    if normalized not in valid:
        raise ValueError(
            f"Invalid platform serial identifier for {collection.value}: {normalized}. "
            f"Valid options: {sorted(valid)}"
        )
    return normalized


def validate_sensor_mode(collection: CDSECollections, sensor_mode: str) -> str:
    """Validate sensor mode against the collection and return normalized value."""
    if not sensor_mode:
        return sensor_mode
    if collection == CDSECollections.sentinel2:
        valid = {s.value for s in S2SensorModes}
    elif collection == CDSECollections.sentinel1:
        valid = {s.value for s in S1SensorModes}
    else:
        raise ValueError(
            f"Unsupported collection for sensor mode validation: {collection.value}"
        )

    normalized = sensor_mode.upper()
    if normalized not in valid:
        raise ValueError(
            f"Invalid sensor mode for {collection.value}: {normalized}. "
            f"Valid options: {sorted(valid)}"
        )
    return normalized


def default_bands(collection: CDSECollections) -> list[str]:
    return COLLECTION_DEFAULTS.get(collection, [])
