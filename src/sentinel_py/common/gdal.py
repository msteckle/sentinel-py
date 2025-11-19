from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping

from lxml import etree


def add_python_pixelfunc_to_vrt(
    vrt_path: Path,
    func_name: str,
    func_code: str,
    *,
    band: int = 1,
    args: Mapping[str, str] | None = None,
) -> None:
    """
    Edit a VRT in-place to turn a band into a Python PixelFunction band.

    - Keeps all existing georef, metadata, SimpleSource, etc.
    - Sets subClass="VRTDerivedRasterBand"
    - Injects PixelFunctionLanguage/Type/Arguments/Code.
    """
    vrt_path = Path(vrt_path)
    tree = etree.parse(str(vrt_path))
    root = tree.getroot()

    band_el = root.find(f".//VRTRasterBand[@band='{band}']")
    if band_el is None:
        raise RuntimeError(f"No VRTRasterBand band='{band}' found in {vrt_path}")

    # Make it a derived band
    band_el.set("subClass", "VRTDerivedRasterBand")

    # Remove any existing pixel-function-related elements
    for tag in (
        "PixelFunctionLanguage",
        "PixelFunctionType",
        "PixelFunctionArguments",
        "PixelFunctionCode",
    ):
        for el in band_el.findall(tag):
            band_el.remove(el)

    # Language
    lang_el = etree.SubElement(band_el, "PixelFunctionLanguage")
    lang_el.text = "Python"

    # Type
    type_el = etree.SubElement(band_el, "PixelFunctionType")
    type_el.text = func_name

    # Arguments
    args_el = etree.SubElement(band_el, "PixelFunctionArguments")
    for k, v in (args or {}).items():
        args_el.set(k, str(v))

    # Code
    code_el = etree.SubElement(band_el, "PixelFunctionCode")
    code_el.text = etree.CDATA(func_code)

    # Write back in place
    tree.write(str(vrt_path), pretty_print=True, xml_declaration=True, encoding="UTF-8")
