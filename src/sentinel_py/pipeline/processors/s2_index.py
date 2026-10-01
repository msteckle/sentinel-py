"""Lazy Sentinel-2 spectral index calculations."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar

import numpy as np
from pydantic import BaseModel, ConfigDict, Field, field_validator

from sentinel_py.cache import deterministic_cache_key
from sentinel_py.pipeline.processors.base import (
    Processor,
    ProcessorConfig,
    ProcessorContext,
)


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
            or any(not isinstance(band, str) or not band for band in bands)
        ):
            raise TypeError(
                f"spectral index {normalized_name} must define non-empty bands"
            )
        if "formula" not in cls.__dict__ or "denominator" not in cls.__dict__:
            raise TypeError(
                f"spectral index {normalized_name} must define formula and denominator"
            )
        cls.name = normalized_name
        cls.bands = tuple(band.upper() for band in bands)
        SpectralIndex.registry[normalized_name] = cls

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate the index values from its selected input bands."""
        raise NotImplementedError

    @classmethod
    def denominator(cls, values: Any) -> Any:
        """Return the denominator used to identify invalid index pixels."""
        raise NotImplementedError


class NDGI(SpectralIndex):
    """Normalized difference greenness index."""

    name = "NDGI"
    bands = ("B03", "B04", "B08")

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate NDGI from Sentinel-2 bands B03, B04, and B08."""
        green = 0.62 * values.sel(band="B03") + 0.38 * values.sel(band="B08")
        return (green - values.sel(band="B04")) / cls.denominator(values)

    @classmethod
    def denominator(cls, values: Any) -> Any:
        """Return the NDGI normalization denominator."""
        green = 0.62 * values.sel(band="B03") + 0.38 * values.sel(band="B08")
        return green + values.sel(band="B04")


class NDWI1(SpectralIndex):
    """Normalized difference water index variant using B08, B11, and B12."""

    name = "NDWI1"
    bands = ("B08", "B11", "B12")

    @classmethod
    def formula(cls, values: Any) -> Any:
        """Calculate NDWI1 from Sentinel-2 bands B08, B11, and B12."""
        return (values.sel(band="B08") - values.sel(band="B11")) / cls.denominator(
            values
        )

    @classmethod
    def denominator(cls, values: Any) -> Any:
        """Return the NDWI1 normalization denominator."""
        return values.sel(band="B08") - values.sel(band="B12")


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
        names = tuple(item.upper() for item in value if isinstance(item, str))
        if len(names) != len(value) or any(not name for name in names):
            raise ValueError("indices must contain non-empty strings")
        if len(set(names)) != len(names):
            raise ValueError("indices must not contain duplicates")
        unsupported = sorted(set(names) - set(SpectralIndex.registry))
        if unsupported:
            raise ValueError("unsupported indices: " + ", ".join(unsupported))
        return names


class S2IndexProcessor(Processor):
    """Append requested spectral index bands to one lazy Sentinel-2 cube."""

    type_name = "s2.index"
    config_model = S2IndexNodeConfig
    version = 1

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
        del sources, output_grid
        if not isinstance(config, S2IndexNodeConfig):
            raise TypeError(f"{node_id} has an invalid s2.index configuration")
        if len(inputs) != 1:
            raise ValueError("s2.index requires exactly one upstream input")
        if any(output.format not in {"cog", "xarray"} for output in outputs):
            raise ValueError("s2.index requires named COG or xarray outputs")

    def execute(
        self,
        config: ProcessorConfig,
        inputs: Mapping[str, Any],
        context: ProcessorContext,
    ) -> Any:
        """Build lazy, per-timestep index calculations without reading pixels."""
        if not isinstance(config, S2IndexNodeConfig):
            raise TypeError("Invalid s2.index configuration")
        if len(inputs) != 1:
            raise ValueError("s2.index requires exactly one upstream input")

        try:
            import xarray as xr
        except ImportError as error:
            raise RuntimeError("Index processing requires xarray") from error

        source = next(iter(inputs.values()))
        if not isinstance(source, xr.Dataset) or "reflectance" not in source:
            raise TypeError("s2.index input must be an xarray Dataset with reflectance")
        reflectance = source["reflectance"]
        if "band" not in reflectance.dims:
            raise ValueError("s2.index input reflectance must have a band dimension")

        available = {str(value) for value in reflectance.band.values}
        required = tuple(
            dict.fromkeys(
                band
                for index_name in config.indices
                for band in SpectralIndex.registry[index_name].bands
            )
        )
        missing = tuple(band for band in required if band not in available)
        if missing:
            raise ValueError("s2.index input is missing bands: " + ", ".join(missing))

        values = reflectance.sel(band=list(required)).astype(np.float32)
        nodata = reflectance.attrs.get("nodata")
        if nodata is not None:
            values = values.where(values != nodata)

        index_arrays = []
        for index_name in config.indices:
            index_class = SpectralIndex.registry[index_name]
            selected = values.sel(band=list(index_class.bands))
            result = index_class.formula(selected)
            denominator = index_class.denominator(selected)
            result = result.where(denominator != 0).astype(np.float32)
            result.name = "reflectance"
            result.attrs.update(
                long_name=f"Sentinel-2 {index_name} spectral index",
                index_name=index_name,
                nodata=nodata,
            )
            index_arrays.append(result.expand_dims(band=[index_name]))

        combined = xr.concat(
            [reflectance.astype(np.float32), *index_arrays], dim="band"
        )
        combined.attrs.update(reflectance.attrs)
        combined.attrs["nodata"] = nodata

        result = source.drop_vars("reflectance").assign_coords(band=combined.band)
        result["reflectance"] = combined
        source_recipe = source.attrs.get("recipe_id")
        recipe_id = deterministic_cache_key(
            {
                "processor": self.type_name,
                "version": self.version,
                "indices": config.indices,
                "source_recipe": source_recipe,
            }
        )[:12]
        if "source_fingerprint" in source.coords:
            fingerprints = [
                deterministic_cache_key(
                    {
                        "source": str(value),
                        "indices": config.indices,
                        "recipe_id": recipe_id,
                    }
                )
                for value in source.source_fingerprint.values
            ]
            result = result.assign_coords(source_fingerprint=("time", fingerprints))
        result.attrs.update(
            source.attrs,
            processor=self.type_name,
            indices=config.indices,
            recipe_id=recipe_id,
        )
        context.logger.info(
            "Built index node=%s indices=%s timesteps=%d",
            context.node.node_id,
            ",".join(config.indices),
            result.sizes.get("time", 0),
        )
        return result
