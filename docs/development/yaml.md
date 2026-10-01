# Add YAML Configurations

In the ``sentinel-py run`` command, users supply a YAML configuration file to initiate a processing pipeline. The YAML file is divided into sections, each corresponding to a specific aspect of the pipeline configuration. To change, add, or remove allowed sections of the YAML, users need to modify the ``pipeline/config.py`` file and the corresponding frozen dataclasses.

There is one dataclass for each section of the YAML configuration file. For example, the ``SelectionConfig`` dataclass corresponds to the ``selection`` section of the YAML, and dictates what keys are allowed under that section, plus validation rules for each key's associated value(s). The dataclass should be frozen and is named after the section it represents, with the suffix ``Config``. The ``selection`` section is used to specify a user's study area of interest (AOI), the years of interest, and the start and end of season (or any set period) within each year. That information is used to subselect the relevant data to use for the processing pipeline. At the start of a new dataclass, developers should specify what keys are needed and their allowed data types:

```python
@dataclass(frozen=True)
class SelectionConfig:
    """
    Data class representing spatial and temporal selections.

    Arguments
    ---------
    aoi : Path | None
        Area of interest file path (default is None).
    years : tuple[int, ...] | None
        Years to select (default is None).
    start_period : tuple[int, int]
        Start period as a tuple of (month, day).
    end_period : tuple[int, int]
        End period as a tuple of (month, day).
    """

    aoi: Path | None
    years: tuple[int, ...] | None
    start_period: tuple[int, int]
    end_period: tuple[int, int]
```

Then, users should create a classmethod that, given a raw mapping (typically parsed from the YAML file), plus any other necessary information, will validate the contents of the mapping and return an instance of the dataclass. The ``SelectionConfig`` classmethod parses the raw mapping for the ``selection`` section, and validates its keys and values. In this case, we check that an expected set of keys are present using ``_only_keys``, we check that the AOI path exists and is a file, we validate the years selection, and we ensure the start and end periods are chronologically ordered. We raise helpful errors so that users can quickly identify and correct any issues in their YAML configuration.

```python

    @classmethod
    def from_mapping(cls, raw: Any, base_dir: Path):

        # Extract the "selection" section from the raw mapping and validate its keys
        value = _mapping(raw, "selection")
        _only_keys(value, {"aoi", "years", "speriod", "eperiod"}, "selection")

        # Resolve the area of interest (AOI) file path and validate its existence
        aoi = (
            _resolve_path(value["aoi"], base_dir, "selection.aoi")
            if value.get("aoi") is not None
            else None
        )
        if aoi is not None and not aoi.is_file():
            raise PipelineConfigError(f"selection.aoi does not exist: {aoi}")

        # Process the years selection and validate the values
        raw_years = value.get("years")
        years = None
        if raw_years is not None:
            items = _sequence(raw_years, "selection.years")
            if not items:
                raise PipelineConfigError("selection.years cannot be empty")
            if any(
                isinstance(year, bool) or not isinstance(year, int) for year in items
            ):
                raise PipelineConfigError("selection.years must contain integers")
            years = tuple(sorted(set(items)))
            if any(year < 2015 or year > 9999 for year in years):
                raise PipelineConfigError(
                    "selection.years must be between 2015 and 9999"
                )

        # Process the start and end periods and validate the chronological order
        start = _season(value.get("speriod", "01-01"), "selection.speriod")
        end = _season(value.get("eperiod", "12-31"), "selection.eperiod")
        if end < start:
            raise PipelineConfigError(
                "selection.eperiod must be on or after selection.speriod"
            )

        # Return the validated values
        return cls(aoi=aoi, years=years, start_period=start, end_period=end)
```