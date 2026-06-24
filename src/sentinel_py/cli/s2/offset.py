from pathlib import Path
from typing import Annotated, Optional

import typer

app = typer.Typer(
    help="Commands for computing DN offsets and creating PB-offset VRTs for Sentinel-2."
)


def _bandwise_create_pb_offset_vrt(
    band_path: str,
    dn_offset: int,
    out_dir: str,
    dst_nodata: int,
) -> str:
    """
    Worker used in ProcessPoolExecutor.
    Uses only simple, picklable arguments.
    """
    from s2.mask import create_pb_offset_vrt

    band_p = Path(band_path)
    out_d = Path(out_dir)
    vrt = create_pb_offset_vrt(
        band_jp2_path=band_p,
        dn_offset=dn_offset,
        out_vrt_dir=out_d,
        dst_nodata=dst_nodata,
        logger=None,
    )
    return str(vrt)


@app.command(
    "dn-offset",
    help=(
        "Determine per-band DN offsets for Sentinel-2 Level-2A products "
        "so later temporal composites and mosaics are radiometrically consistent."
    ),
)
def dn_offset(
    input_dir: Annotated[  #
        Path,
        typer.Option(
            exists=True,
            file_okay=False,
            dir_okay=True,
            help="Directory with Sentinel-2 L2A products.",
        ),
    ],
    output_dir: Annotated[
        Path, typer.Option(help="Output directory for DN offset VRT files.")
    ],
    years: Annotated[
        str,
        typer.Option(
            help='Space-separated list of years in quotes. E.g., "2020 2021 2022".'
        ),
    ],
    speriod: Annotated[
        str,
        typer.Option(help="Start of seasonal window as MM-DD. E.g. --speriod 06-01"),
    ],
    eperiod: Annotated[
        str, typer.Option(help="End of seasonal window as MM-DD. E.g. --eperiod 08-31")
    ],
    bands: Annotated[list[str], typer.Option(help="List of bands to process.")] = [
        "B02",
        "B03",
        "B04",
        "B05",
        "B06",
        "B07",
        "B08",
        "B8A",
        "B11",
        "B12",
    ],
    res: Annotated[
        int, typer.Option(help="Target resolution in meters: 10, 20, or 60.")
    ] = 20,
    log: Annotated[
        Optional[Path],
        typer.Option(
            help="Optional log file path. If omitted, default logging config is used."
        ),
    ] = None,
    verbose: Annotated[
        bool, typer.Option(help="Enable verbose logging to the console.")
    ] = False,
    n_workers: Annotated[int, typer.Option(help="Number of parallel workers.")] = 4,
    dst_nodata: Annotated[
        int, typer.Option(help="Nodata value to write into PB-offset VRTs.")
    ] = 65535,
):
    """
    In parallel, compute per-band DN offsets for Sentinel-2 L2A products
    and write PB-offset VRTs.
    """
    from concurrent.futures import ProcessPoolExecutor, as_completed

    from log import get_logger
    from s2.mask import get_band_paths, get_pb_offset_from_jp2
    from utils import parse_years

    # set up logging
    logger = get_logger(name="download_logger", logpath=log, verbose=verbose)

    # Ensure output directory exists
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Parse years and seasonal window
    years_set = parse_years(years)
    try:
        start_month, start_day = map(int, speriod.split("-"))
        end_month, end_day = map(int, eperiod.split("-"))
    except Exception as exc:
        raise typer.BadParameter(
            f"Could not parse period_start/period_end as MM-DD: {speriod}, {eperiod}"
        ) from exc
    start_md = (start_month, start_day)
    end_md = (end_month, end_day)

    # get all jp2 band paths
    band_paths_df = get_band_paths(
        input_dir,
        bands,
        res,
        years=years_set,
        period_start=start_md,
        period_end=end_md,
        logger=logger,
    )

    # Compute DN offsets for each band
    band_paths_df["dn_offset"] = band_paths_df["band_jp2_path"].apply(
        lambda p: get_pb_offset_from_jp2(Path(p), logger=logger)
    )

    # In parallel, create PB-offset VRTs
    tasks = [
        (str(Path(row["band_jp2_path"])), int(row["dn_offset"]))
        for _, row in band_paths_df.iterrows()
    ]
    if logger:
        logger.info(
            f"Starting PB-offset VRT creation for {len(tasks)} bands "
            f"using {n_workers} workers. Output dir: {output_dir}"
        )

    # Assign tasks to workers and collects failures
    failures: list[tuple[Path, Exception]] = []
    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        future_to_band = {
            ex.submit(
                _bandwise_create_pb_offset_vrt,
                band_path,
                dn_off,
                str(output_dir),
                dst_nodata,
            ): Path(band_path)
            for band_path, dn_off in tasks
        }

        for fut in as_completed(future_to_band):
            band_path = future_to_band[fut]
            try:
                vrt_path = Path(fut.result())
                if logger:
                    logger.info(f"Created PB-offset VRT for {band_path} -> {vrt_path}")
            except Exception as exc:
                failures.append((band_path, exc))
                if logger:
                    logger.error(f"Failed PB-offset VRT for {band_path}: {exc!r}")

    if failures:
        typer.echo(
            f"Completed with {len(failures)} failures out of {len(tasks)} bands.",
            err=True,
        )
    else:
        typer.echo(f"Successfully processed {len(tasks)} bands.")
