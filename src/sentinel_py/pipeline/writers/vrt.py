"""Virtual mosaics (VRT) of the COG tiles written by the COG output writer.

A VRT is a small XML file that lists tiles and where they sit, so it stores no pixels.
GDAL readers (QGIS included) open it as one raster and read through the tiles' own
overviews when zoomed out. The writer builds one VRT per AOI unit, time bin and
variable, and one mosaic per output that lists the tiles of every AOI unit.

rasterio has no public mosaic-VRT builder (``rasterio.vrt`` only offers
``WarpedVRT``), so the XML is built here with the standard library, the same way
rasterio's own private helper builds its VRT documents.
"""

from __future__ import annotations

import logging
import os
import uuid
import xml.etree.ElementTree as ET
from collections.abc import Sequence
from dataclasses import dataclass, replace
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

# GDAL data type names of the dtypes the COG writer produces.
GDAL_DATA_TYPES = {
    "uint8": "Byte",
    "uint16": "UInt16",
    "int16": "Int16",
    "uint32": "UInt32",
    "int32": "Int32",
    "float32": "Float32",
    "float64": "Float64",
}
# COG internal block size (see ``_write_cog_tile``); lets GDAL open a VRT without
# opening every source tile first.
SOURCE_BLOCK_SIZE = 256


########################################################################################
# Records
########################################################################################


@dataclass(frozen=True)
class VRTTile:
    """One written tile and where it sits in the output grid.

    Attributes
    ----------
    path : Path
        Tile file.
    west, north : float
        Projected coordinates of the tile's upper-left corner.
    width, height : int
        Tile size in pixels.
    """

    path: Path
    west: float
    north: float
    width: int
    height: int


@dataclass(frozen=True)
class VRTSpec:
    """Everything needed to mosaic the tiles of one (time bin, variable).

    Attributes
    ----------
    product_id, granule_id, variable : str
        Identify the time bin and variable; they name the VRT file
        ``<product_id>/<granule_id>/<variable>.vrt``.
    crs : str
        Coordinate reference system of the tiles.
    resolution : tuple[float, float]
        Positive pixel size ``(x, y)`` in CRS units.
    dtype : str
        Stored numpy dtype name of the tiles (after any int packing).
    nodata : float
        Stored nodata value (NaN for floating-point tiles).
    descriptions : tuple[str, ...]
        Band descriptions.
    scale, offset : float | None
        GDAL scale and offset of packed tiles; ``None`` for unpacked tiles.
    tiles : tuple[VRTTile, ...]
        The tiles of this (time bin, variable).
    """

    product_id: str
    granule_id: str
    variable: str
    crs: str
    resolution: tuple[float, float]
    dtype: str
    nodata: float
    descriptions: tuple[str, ...]
    scale: float | None
    offset: float | None
    tiles: tuple[VRTTile, ...]

    @property
    def key(self) -> tuple[str, str, str]:
        """Return the ``(product_id, granule_id, variable)`` that names the VRT."""
        return self.product_id, self.granule_id, self.variable

    @property
    def layout(self) -> tuple:
        """Return the properties that tiles must share to be mosaicked together."""
        nodata = "nan" if np.isnan(self.nodata) else self.nodata
        return (
            self.crs, self.resolution, self.dtype, nodata, self.descriptions,
            self.scale, self.offset,
        )


########################################################################################
# XML construction
########################################################################################


def _number(value: float) -> str:
    """Format a number for VRT XML (``nan`` for NaN, integers without a fraction)."""
    if np.isnan(value):
        return "nan"
    return str(int(value)) if float(value).is_integer() else repr(float(value))


def build_vrt(specs: Sequence[VRTSpec], vrt_path: Path) -> ET.Element:
    """Build the VRT document mosaicking every tile of ``specs``.

    Parameters
    ----------
    specs : sequence of VRTSpec
        Specs with the same ``layout``: one spec for a per-AOI VRT, one per AOI unit
        for an output mosaic. Where tiles overlap, later specs draw over earlier ones.
    vrt_path : Path
        Where the VRT will be written; tile paths are stored relative to it, so the
        output directory can be moved or shared.

    Returns
    -------
    xml.etree.ElementTree.Element
        The ``VRTDataset`` element.

    Raises
    ------
    ValueError
        If there are no tiles, or the specs differ in layout.
    """
    from rasterio.crs import CRS

    if not specs or not any(spec.tiles for spec in specs):
        raise ValueError("A VRT needs at least one tile")
    first = specs[0]
    if any(spec.layout != first.layout for spec in specs):
        raise ValueError("VRT specs must share CRS, resolution, dtype, nodata and bands")
    x_res, y_res = first.resolution
    tiles = [tile for spec in specs for tile in spec.tiles]
    west = min(tile.west for tile in tiles)
    north = max(tile.north for tile in tiles)
    east = max(tile.west + tile.width * x_res for tile in tiles)
    south = min(tile.north - tile.height * y_res for tile in tiles)

    root = ET.Element(
        "VRTDataset",
        rasterXSize=str(round((east - west) / x_res)),
        rasterYSize=str(round((north - south) / y_res)),
    )
    ET.SubElement(root, "SRS").text = CRS.from_user_input(first.crs).to_wkt()
    ET.SubElement(root, "GeoTransform").text = ", ".join(
        repr(float(value)) for value in (west, x_res, 0.0, north, 0.0, -y_res)
    )
    data_type = GDAL_DATA_TYPES[first.dtype]
    nodata = _number(first.nodata)
    for band, description in enumerate(first.descriptions, start=1):
        element = ET.SubElement(
            root, "VRTRasterBand", dataType=data_type, band=str(band)
        )
        ET.SubElement(element, "NoDataValue").text = nodata
        ET.SubElement(element, "Description").text = description
        if first.scale is not None:
            ET.SubElement(element, "Offset").text = _number(first.offset or 0.0)
            ET.SubElement(element, "Scale").text = _number(first.scale)
        ET.SubElement(element, "ColorInterp").text = "Gray"
        for tile in tiles:
            source = ET.SubElement(element, "ComplexSource")
            ET.SubElement(source, "SourceFilename", relativeToVRT="1").text = (
                Path(os.path.relpath(tile.path, vrt_path.parent)).as_posix()
            )
            ET.SubElement(source, "SourceBand").text = str(band)
            ET.SubElement(
                source,
                "SourceProperties",
                RasterXSize=str(tile.width),
                RasterYSize=str(tile.height),
                DataType=data_type,
                BlockXSize=str(min(SOURCE_BLOCK_SIZE, tile.width)),
                BlockYSize=str(min(SOURCE_BLOCK_SIZE, tile.height)),
            )
            ET.SubElement(
                source, "SrcRect",
                xOff="0", yOff="0", xSize=str(tile.width), ySize=str(tile.height),
            )
            ET.SubElement(
                source, "DstRect",
                xOff=str(round((tile.west - west) / x_res)),
                yOff=str(round((north - tile.north) / y_res)),
                xSize=str(tile.width), ySize=str(tile.height),
            )
            ET.SubElement(source, "NODATA").text = nodata
    return root


def write_vrt(specs: Sequence[VRTSpec], vrt_path: Path) -> Path:
    """Write the VRT mosaicking ``specs`` atomically and return its path.

    Parameters
    ----------
    specs : sequence of VRTSpec
        Specs to mosaic (see :func:`build_vrt`).
    vrt_path : Path
        Destination file; its directory is created if needed.

    Returns
    -------
    Path
        ``vrt_path``.
    """
    root = build_vrt(specs, vrt_path)
    ET.indent(root)
    vrt_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = vrt_path.with_name(f".{vrt_path.name}.{uuid.uuid4().hex}.tmp")
    try:
        ET.ElementTree(root).write(temporary, encoding="utf-8")
        os.replace(temporary, vrt_path)
    finally:
        temporary.unlink(missing_ok=True)
    return vrt_path


########################################################################################
# Writing the VRTs of one output
########################################################################################


def vrt_path(base: Path, spec: VRTSpec) -> Path:
    """Return ``<base>/<product_id>/<granule_id>/<variable>.vrt`` for a spec."""
    return base / spec.product_id / spec.granule_id / f"{spec.variable}.vrt"


def write_unit_vrts(
    specs: Sequence[VRTSpec], base: Path, failed: set[Path]
) -> list[Path]:
    """Write one VRT per spec of an AOI unit whose tiles were all written.

    Parameters
    ----------
    specs : sequence of VRTSpec
        The unit's specs, one per (time bin, variable).
    base : Path
        The unit's output directory.
    failed : set of Path
        Tile paths whose write failed; a spec with a failed or missing tile gets no
        VRT, so a partial mosaic is never presented as complete.

    Returns
    -------
    list[Path]
        The VRTs written.
    """
    written = []
    for spec in specs:
        incomplete = [
            tile.path for tile in spec.tiles
            if tile.path in failed or not tile.path.is_file()
        ]
        if incomplete:
            logger.warning(
                "Skipping VRT %s: %d tile(s) failed or are missing",
                vrt_path(base, spec).name, len(incomplete),
            )
            continue
        written.append(write_vrt([spec], vrt_path(base, spec)))
    return written


def write_output_mosaics(
    unit_specs: Sequence[Sequence[VRTSpec]], base: Path, failed: set[Path]
) -> list[Path]:
    """Write one mosaic VRT per (time bin, variable) over every AOI unit's tiles.

    Parameters
    ----------
    unit_specs : sequence of sequence of VRTSpec
        The specs of each AOI unit.
    base : Path
        The output directory the mosaics are written under.
    failed : set of Path
        Failed tile paths; they are left out, so one bad site leaves a gap rather
        than blocking the layer.

    Returns
    -------
    list[Path]
        The mosaics written. Units whose layout differs from the first unit of the
        same (time bin, variable) are left out with a warning.
    """
    groups: dict[tuple[str, str, str], list[VRTSpec]] = {}
    for specs in unit_specs:
        for spec in specs:
            usable = tuple(
                tile for tile in spec.tiles
                if tile.path not in failed and tile.path.is_file()
            )
            if not usable:
                continue
            group = groups.setdefault(spec.key, [])
            if group and group[0].layout != spec.layout:
                logger.warning(
                    "Leaving a unit out of the %s mosaic: its CRS, pixel size, "
                    "dtype, nodata or bands differ", "/".join(spec.key),
                )
                continue
            group.append(replace(spec, tiles=usable))
    return [
        write_vrt(group, vrt_path(base, group[0])) for group in groups.values()
    ]
