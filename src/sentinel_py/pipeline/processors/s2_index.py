"""Lazy Sentinel-2 spectral index calculations."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar

import numpy as np
from pydantic import BaseModel, ConfigDict, Field, field_validator

from sentinel_py.cache import deterministic_cache_key
from sentinel_py.enums import S2_NODATA
from sentinel_py.pipeline.processors.base import (
    BaseProcessor,
    make_recipe_id,
    mask_nodata,
    ProcessorConfig,
    ProcessorContext,
)

PROCESSOR_VERSION = 2


########################################################################################
# Index calculation helpers
########################################################################################


def _safe_divide(numerator: Any, denominator: Any) -> Any:
    """Divide array-like values while masking zero denominators.

    Parameters
    ----------
    numerator
        Array-like numerator supporting arithmetic.
    denominator
        Array-like denominator supporting ``where``.

    Returns
    -------
    Any
        The quotient, with zero-denominator positions represented as missing.
    """
    return numerator / denominator.where(denominator != 0)


def _raster_reflectance(source: Any, reflectance: Any) -> Any:
    """Convert Sentinel-2 digital numbers to reflectance when scale is available.

    Parameters
    ----------
    source
        Source xarray dataset, potentially carrying a per-time-step
        ``quantification_value`` coordinate.
    reflectance
        Reflectance data array selected from ``source``.

    Returns
    -------
    Any
        Floating-point reflectance values suitable for spectral formulas.
    """
    values = reflectance.astype(np.float32)
    values = mask_nodata(values, reflectance.attrs.get("nodata"))
    quantification = source.coords.get("quantification_value")
    if quantification is not None:
        return _safe_divide(values, quantification.astype(np.float32))
    if "reflectance dn" in str(reflectance.attrs.get("long_name", "")).lower():
        return values / np.float32(10_000)
    return values


########################################################################################
# Formulas for Sentinel-2 spectral indices
########################################################################################


class SpectralIndex:
    """Base class and automatic registry for one Sentinel-2 spectral index."""

    registry: ClassVar[dict[str, type[SpectralIndex]]] = {}
    name: ClassVar[str]
    bands: ClassVar[tuple[str, ...]]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Validate and register every concrete spectral-index subclass."""
        super().__init_subclass__(**kwargs)
        name = cls.__dict__.get("name")
        bands = cls.__dict__.get("bands")
        if not isinstance(name, str) or not name.strip():
            raise TypeError("spectral index subclasses must define a non-empty name")
        normalized_name = name.strip().upper()
        if normalized_name in SpectralIndex.registry:
            raise ValueError(f"duplicate spectral index name: {normalized_name}")
        if (
            not isinstance(bands, tuple)
            or not bands
            or any(not isinstance(band, str) or not band.strip() for band in bands)
        ):
            raise TypeError(
                f"spectral index {normalized_name} must define non-empty bands"
            )
        normalized_bands = tuple(band.strip().upper() for band in bands)
        if len(set(normalized_bands)) != len(normalized_bands):
            raise ValueError(
                f"spectral index {normalized_name} must not repeat input bands"
            )
        if "formula" not in cls.__dict__:
            raise TypeError(f"spectral index {normalized_name} must define formula")
        cls.name = normalized_name
        cls.bands = normalized_bands
        SpectralIndex.registry[normalized_name] = cls

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate the index values from its selected input bands."""
        raise NotImplementedError


class NDGI(SpectralIndex):
    """
    Normalized Difference Greenness Index.
    https://doi-org.ornl.idm.oclc.org/10.1016/j.rse.2019.03.028
    """

    name = "NDGI"
    bands = ("B03", "B04", "B08")

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate NDGI from reflectance with a ``band`` dimension."""
        red = values.sel(band="B04")
        green = values.sel(band="B03")
        nir = values.sel(band="B08")
        return _safe_divide(
            0.62 * green + 0.38 * nir - red, 0.62 * green + 0.38 * nir + red
        )


class NDWI1(SpectralIndex):
    """
    Normalized Difference Water Index variant using SWIR1.
    https://docs.geopera.com/spectral-indices/ndwi_gao
    """

    name = "NDWI1"
    bands = ("B08", "B11")

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate NDWI1 from reflectance with a ``band`` dimension."""
        nir = values.sel(band="B08")
        swir1 = values.sel(band="B11")
        return _safe_divide(nir - swir1, nir + swir1)


class NDWI2(SpectralIndex):
    """
    Normalized Difference Water Index variant using SWIR2.
    https://docs.geopera.com/spectral-indices/ndwi_gao
    """

    name = "NDWI2"
    bands = ("B08", "B12")

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate NDWI2 from reflectance with a ``band`` dimension."""
        nir = values.sel(band="B08")
        swir2 = values.sel(band="B12")
        return _safe_divide(nir - swir2, nir + swir2)


class MSAVI(SpectralIndex):
    """
    Modified Soil Adjusted Vegetation Index.
    https://docs.geopera.com/spectral-indices/msavi?sensor=sentinel-2
    """

    name = "MSAVI"
    bands = ("B04", "B08")

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate MSAVI from reflectance with a ``band`` dimension."""
        nir = values.sel(band="B08")
        red = values.sel(band="B04")
        discriminant = (2 * nir + 1) ** 2 - 8 * (nir - red)
        root = np.sqrt(discriminant.where(discriminant >= 0))
        return 0.5 * (2 * nir + 1 - root)


class VARI(SpectralIndex):
    """
    Visible Atmospherically Resistant Index.
    https://docs.geopera.com/spectral-indices/vari_visible?sensor=sentinel-2
    """

    name = "VARI"
    bands = ("B04", "B03", "B02")

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate VARI from reflectance with a ``band`` dimension."""
        red = values.sel(band="B04")
        green = values.sel(band="B03")
        blue = values.sel(band="B02")
        return _safe_divide(green - red, green + red - blue)


class RVI(SpectralIndex):
    """
    Ratio Vegetation Index.
    https://docs.geopera.com/spectral-indices/rvi
    """

    name = "RVI"
    bands = ("B08", "B04")

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate RVI from reflectance with a ``band`` dimension."""
        nir = values.sel(band="B08")
        red = values.sel(band="B04")
        return _safe_divide(nir, red)


class OSAVI(SpectralIndex):
    """
    Optimized Soil Adjusted Vegetation Index.
    """

    name = "OSAVI"
    bands = ("B08", "B04")

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate OSAVI from reflectance with a ``band`` dimension."""
        nir = values.sel(band="B08")
        red = values.sel(band="B04")
        return _safe_divide(1.16 * (nir - red), nir + red + 0.16)


class TGI(SpectralIndex):
    """
    Triangular Greenness Index.
    https://docs.geopera.com/spectral-indices/tgi
    """

    name = "TGI"
    bands = ("B04", "B03", "B02")

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate TGI from reflectance with a ``band`` dimension."""
        red = values.sel(band="B04")
        green = values.sel(band="B03")
        blue = values.sel(band="B02")
        return -0.5 * (190 * (red - green) - 120 * (red - blue))


class GLI(SpectralIndex):
    """
    Green Leaf Index.
    """

    name = "GLI"
    bands = ("B04", "B03", "B02")

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate GLI from reflectance with a ``band`` dimension."""
        red = values.sel(band="B04")
        green = values.sel(band="B03")
        blue = values.sel(band="B02")
        return _safe_divide(2 * green - red - blue, 2 * green + red + blue)


class NGRDI(SpectralIndex):
    """
    Normalized Green-Red Difference Index.
    """

    name = "NGRDI"
    bands = ("B03", "B04")

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate NGRDI from reflectance with a ``band`` dimension."""
        red = values.sel(band="B04")
        green = values.sel(band="B03")
        return _safe_divide(green - red, green + red)


class CIG(SpectralIndex):
    """
    Chlorophyll Index (Green).
    """

    name = "CIG"
    bands = ("B08", "B03")

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate CIG from reflectance with a ``band`` dimension."""
        nir = values.sel(band="B08")
        green = values.sel(band="B03")
        return _safe_divide(nir, green) - 1


class GNDVI(SpectralIndex):
    """
    Green Normalized Difference Vegetation Index.
    """

    name = "GNDVI"
    bands = ("B08", "B03")

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate GNDVI from reflectance with a ``band`` dimension."""
        nir = values.sel(band="B08")
        green = values.sel(band="B03")
        return _safe_divide(nir - green, nir + green)


class CVI(SpectralIndex):
    """
    Chlorophyll Vegetation Index.
    """

    name = "CVI"
    bands = ("B08", "B04", "B03")

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate CVI from reflectance with a ``band`` dimension."""
        nir = values.sel(band="B08")
        red = values.sel(band="B04")
        green = values.sel(band="B03")
        return _safe_divide(nir * red, green**2)


class MTVI2(SpectralIndex):
    """
    Modified Triangular Vegetation Index 2.
    """

    name = "MTVI2"
    bands = ("B08", "B04", "B03")

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate MTVI2 from reflectance with a ``band`` dimension."""
        nir = values.sel(band="B08")
        red = values.sel(band="B04")
        green = values.sel(band="B03")
        red_root = np.sqrt(red.where(red >= 0))
        discriminant = (2 * nir + 1) ** 2 - (6 * nir - 5 * red_root) - 0.5
        denominator = np.sqrt(discriminant.where(discriminant > 0))
        return _safe_divide(
            1.5 * (1.2 * (nir - green) - 2.5 * (red - green)),
            denominator,
        )


class TCARI(SpectralIndex):
    """
    Transformed Chlorophyll Absorption in Reflectance Index.
    """

    name = "TCARI"
    bands = ("B05", "B04", "B03")

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate TCARI from reflectance with a ``band`` dimension."""
        re1 = values.sel(band="B05")
        red = values.sel(band="B04")
        green = values.sel(band="B03")
        correction = _safe_divide((re1 - green) * re1, red)
        return 3 * ((re1 - red) - 0.2 * correction)


class TCI(SpectralIndex):
    """
    Triangular Chlorophyll Index.
    """

    name = "TCI"
    bands = ("B05", "B04", "B03")

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate TCI from reflectance with a ``band`` dimension."""
        re1 = values.sel(band="B05")
        red = values.sel(band="B04")
        green = values.sel(band="B03")
        ratio = _safe_divide(re1, red)
        ratio_root = np.sqrt(ratio.where(ratio >= 0))
        return 1.2 * (re1 - green) - 1.5 * (red - green) * ratio_root


class NARI(SpectralIndex):
    """
    Normalized Anthocyanin Reflectance Index.
    """

    name = "NARI"
    bands = ("B05", "B03")

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate NARI from reflectance with a ``band`` dimension."""
        re1 = values.sel(band="B05")
        green = values.sel(band="B03")
        return _safe_divide(re1 - green, re1 + green)


class TCAPB(SpectralIndex):
    """
    Tasseled Cap Brightness Index.
    """

    name = "TCAPB"
    bands = ("B02", "B03", "B04", "B08", "B11", "B12")

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate TCAPB from reflectance with a ``band`` dimension."""
        blue = values.sel(band="B02")
        green = values.sel(band="B03")
        red = values.sel(band="B04")
        nir = values.sel(band="B08")
        swir1 = values.sel(band="B11")
        swir2 = values.sel(band="B12")
        return (
            0.3037 * blue
            + 0.2793 * green
            + 0.4743 * red
            + 0.5585 * nir
            + 0.5082 * swir1
            + 0.1863 * swir2
        )


class TCAPG(SpectralIndex):
    """
    Tasseled Cap Greenness Index.
    """

    name = "TCAPG"
    bands = ("B02", "B03", "B04", "B08", "B11", "B12")

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate TCAPG from reflectance with a ``band`` dimension."""
        blue = values.sel(band="B02")
        green = values.sel(band="B03")
        red = values.sel(band="B04")
        nir = values.sel(band="B08")
        swir1 = values.sel(band="B11")
        swir2 = values.sel(band="B12")
        return (
            -0.2848 * blue
            - 0.2435 * green
            - 0.5436 * red
            + 0.7243 * nir
            + 0.0840 * swir1
            - 0.1800 * swir2
        )


class TCAPW(SpectralIndex):
    """
    Tasseled Cap Wetness Index.
    """

    name = "TCAPW"
    bands = ("B02", "B03", "B04", "B08", "B11", "B12")

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate TCAPW from reflectance with a ``band`` dimension."""
        blue = values.sel(band="B02")
        green = values.sel(band="B03")
        red = values.sel(band="B04")
        nir = values.sel(band="B08")
        swir1 = values.sel(band="B11")
        swir2 = values.sel(band="B12")
        return (
            0.1509 * blue
            + 0.1973 * green
            + 0.3279 * red
            + 0.3406 * nir
            - 0.7112 * swir1
            - 0.4572 * swir2
        )


########################################################################################
# Processor class for calculating Sentinel-2 spectral indices
########################################################################################


class S2IndexNodeConfig(BaseModel):
    """Validated parameters belonging specifically to the ``s2.index`` node."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    indices: tuple[str, ...] = Field(min_length=1)

    @field_validator("indices", mode="before")
    @classmethod
    def validate_indices(cls, value: Any) -> tuple[str, ...]:
        """Normalize index names and reject unsupported or repeated names."""
        if not isinstance(value, (list, tuple)):
            raise TypeError("indices must be a list")
        names = tuple(item.strip().upper() for item in value if isinstance(item, str))
        if len(names) != len(value) or any(not name for name in names):
            raise ValueError("indices must contain non-empty strings")
        if len(set(names)) != len(names):
            raise ValueError("indices must not contain duplicates")
        unsupported = sorted(set(names) - set(SpectralIndex.registry))
        if unsupported:
            raise ValueError("unsupported indices: " + ", ".join(unsupported))
        return names


def _append_indices(source: Any, config: S2IndexNodeConfig) -> Any:
    """Append configured indices to one lazy xarray dataset."""
    import xarray as xr

    # Validate that the source dataset contains the necessary reflectance data & bands
    if not isinstance(source, xr.Dataset) or "reflectance" not in source:
        raise TypeError("s2.index input must be an xarray Dataset with reflectance")
    reflectance = source["reflectance"]
    if "band" not in reflectance.dims:
        raise ValueError("s2.index input reflectance must have a band dimension")

    # Gather available and required bands for the requested indices
    available = {str(value) for value in reflectance.band.values}
    required = tuple(
        dict.fromkeys(
            band
            for index_name in config.indices
            for band in SpectralIndex.registry[index_name].bands
        )
    )

    # Check for missing bands required by the requested indices
    missing = tuple(band for band in required if band not in available)
    if missing:
        raise ValueError("s2.index input is missing bands: " + ", ".join(missing))

    # Convert DN to reflectance before applying the index calculations
    values = _raster_reflectance(source, reflectance.sel(band=list(required)))
    nodata = reflectance.attrs.get("nodata")
    arrays = []

    # Apply the index calculations to the reflectance values
    for index_name in config.indices:
        index_class = SpectralIndex.registry[index_name]
        selected = values.sel(band=list(index_class.bands))
        result = index_class.formula(selected).astype(np.float32)
        result.name = "reflectance"
        result.attrs.update(
            long_name=f"Sentinel-2 {index_name} spectral index",
            index_name=index_name,
            nodata=nodata,
        )
        arrays.append(result.expand_dims(band=[index_name]))

    # Combine the original reflectance data with the newly calculated index arrays
    combined = xr.concat([reflectance.astype(np.float32), *arrays], dim="band")
    combined.attrs.update(reflectance.attrs)
    combined.attrs["nodata"] = nodata
    result = source.drop_vars("reflectance").assign_coords(band=combined.band)
    result["reflectance"] = combined

    # Generate ID for the processed dataset based on the input and configuration
    recipe_id = make_recipe_id(
        "s2.index",
        PROCESSOR_VERSION,
        indices=config.indices,
        source_recipe=source.attrs.get("recipe_id"),
    )

    # Assign the generated recipe ID to the result dataset
    if "source_fingerprint" in source.coords:
        result = result.assign_coords(
            source_fingerprint=(
                "time",
                [
                    deterministic_cache_key(
                        {
                            "source": str(value),
                            "indices": config.indices,
                            "recipe_id": recipe_id,
                        }
                    )
                    for value in source.source_fingerprint.values
                ],
            )
        )
    result.attrs.update(
        source.attrs, processor="s2.index", indices=config.indices, recipe_id=recipe_id
    )
    return result


class S2IndexProcessor(BaseProcessor):
    """Append requested spectral index bands to one lazy Sentinel-2 cube."""

    type_name = "s2.index"
    config_model = S2IndexNodeConfig
    input_kind = None
    version = PROCESSOR_VERSION

    def validate(
        self,
        config: ProcessorConfig,
        *,
        node_id: str,
        inputs: Mapping[str, str],
        sources: Mapping[str, Any],
        outputs: tuple[Any, ...],
        output_grid: Any,
    ) -> None:
        """Validate the graph edge and supported output formats."""
        super().validate(
            config, node_id=node_id, inputs=inputs, sources=sources,
            outputs=outputs, output_grid=output_grid,
        )
        if not isinstance(config, S2IndexNodeConfig):
            raise TypeError(f"{node_id} has an invalid s2.index configuration")
        self.single_input(inputs)

    def execute_raster(
        self,
        config: ProcessorConfig,
        inputs: Mapping[str, Any],
        context: ProcessorContext,
    ) -> Any:
        """Build lazy, per-timestep index calculations without reading pixels."""
        if not isinstance(config, S2IndexNodeConfig):
            raise TypeError("Invalid s2.index configuration")

        try:
            import xarray as xr
        except ImportError as error:
            raise RuntimeError("Index processing requires xarray") from error

        source = self.single_input(inputs)
        from sentinel_py.pipeline.artifacts import RasterArtifact

        if isinstance(source, RasterArtifact):
            result = source.map_datasets(
                lambda dataset, unit: _append_indices(dataset, config)
            )
            context.logger.info(
                "Built index node=%s indices=%s AOI units=%d",
                context.node.node_id,
                ",".join(config.indices),
                len(result.datasets),
            )
            return result
        result = _append_indices(source, config)
        context.logger.info(
            "Built index node=%s indices=%s timesteps=%d",
            context.node.node_id,
            ",".join(config.indices),
            result.sizes.get("time", 0),
        )
        return result

    def execute_points(
        self,
        config: ProcessorConfig,
        inputs: Mapping[str, Any],
        context: ProcessorContext,
    ) -> Any:
        """Append spectral-index columns to normalized point observations."""
        from sentinel_py.pipeline.artifacts import PointSampleArtifact

        if not isinstance(config, S2IndexNodeConfig) or len(inputs) != 1:
            raise TypeError("Invalid s2.index configuration or inputs")
        source = next(iter(inputs.values()))
        if not isinstance(source, PointSampleArtifact):
            raise TypeError("s2.index point input must be a PointSampleArtifact")
        rows = source.rows.copy()
        for index_name in config.indices:
            index = SpectralIndex.registry[index_name]
            missing = [band for band in index.bands if band not in rows]
            if missing:
                raise ValueError(
                    "s2.index input is missing bands: " + ", ".join(missing)
                )
            values = {
                band: rows[band].astype(np.float32).where(rows[band] != S2_NODATA)
                for band in index.bands
            }
            if "quantification_value" in rows:
                scale = rows["quantification_value"].astype(np.float32)
                values = {
                    band: _safe_divide(values[band], scale) for band in index.bands
                }

            class _Values:
                """Minimal band selector shared by point index formulas."""

                def sel(self, *, band: str) -> Any:
                    """Return one point-observation band by name."""
                    return values[band]

            selected = _Values()
            rows[index_name] = index.formula(selected).astype(np.float32)
        return PointSampleArtifact(rows, source.plan)
