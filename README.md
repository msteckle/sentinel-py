### Requirements

- Python 3.13+
- [uv 0.11.7+](https://docs.astral.sh/uv/getting-started/installation/)

Querying and downloading data do not require raster-processing libraries or SNAP.
Sentinel-2 raster processing uses Rasterio, xarray, and Dask through the optional
`processing` dependency described below.

### Installation

1. Clone the repository:

```bash
git clone https://github.com/msteckle/sentinel-py.git
cd sentinel-py
```

2. Create the environment for querying and downloading:

```bash
uv sync --no-dev
```

Run commands through uv, or activate the environment first:

```bash
uv run sentinel-py --help
# or
source .venv/bin/activate
sentinel-py --help
```

To include the optional Sentinel-2 raster-processing dependencies:

```bash
uv sync --no-dev --extra processing
```

For Slurm execution through Dask, install both processing and HPC extras:

```bash
uv sync --no-dev --extra processing --extra hpc
```

### NERSC (Perlmutter)

For query and download work on Perlmutter, no raster-processing extra is needed.
After cloning the repository and installing
[uv](https://docs.astral.sh/uv/getting-started/installation/) in your user account:

```bash
cd sentinel-py
uv sync --no-dev
uv run sentinel-py --help
```

The first command creates `.venv` and installs only the base query/download
dependencies. Add `--extra processing` only for Sentinel-2 processing jobs.

Large downloads should go to Perlmutter scratch rather than your home directory.
For example:

```bash
mkdir -p "$SCRATCH/sentinel-py/data/s1/raw"

uv run sentinel-py asf download \
  --outdir "$SCRATCH/sentinel-py/data/s1/raw" \
  --config "$HOME/.earthdata.netrc" \
  --query "$HOME/.sentinel-py/cache/asf/QUERY_KEY/manifest.parquet"
```

Scratch is intended for temporary, high-performance storage and is not backed up;
move results that must be retained to an appropriate persistent NERSC filesystem.
For shared installations, NERSC recommends a versioned prefix under
`/global/common/software/<project>/`, which is mounted read-only on compute nodes;
keep writable caches and downloaded data outside that installation directory. See
the [NERSC software installation guide](https://docs.nersc.gov/development/installing-sharing-software/)
and [Perlmutter scratch documentation](https://docs.nersc.gov/filesystems/perlmutter-scratch/).

### Downloading S2
To download Sentinel-2 data, you will need to have an account on the [Copernicus Open Access Hub](https://scihub.copernicus.eu/dhus/#/home) and obtain your credentials. Once you have your credentials, you can use the CLI to download data. We recommend exporting your credentials as environment variables for convenience:
```bash
export CDSE_USERNAME=your_username
echo <your_password> > $HOME/.cdse/cdse_pw
chmod 600 $HOME/.cdse/cdse_pw
export CDSE_PASSWORD_FILE=$HOME/.cdse/cdse_pw
```

For the surface-reflectance time-series workflow, query Level-2A products explicitly:

```bash
sentinel-py cdse query \
  --aoi data/aois/toolik_025_aoi.geojson \
  --crs EPSG:4326 \
  --years "2023 2024" \
  --speriod 06-01 \
  --eperiod 08-31 \
  --product S2MSI2A \
  --cloud-thresh 80
```

The released Sentinel-2 user-product choices are `S2MSI1C`, containing
Top-of-Atmosphere reflectance, and `S2MSI2A`, containing Bottom-of-Atmosphere surface
reflectance plus products including the Scene Classification Layer (SCL). Level-2A is
recommended for this workflow. Sentinel-2 relative orbits are numbered 1-143;
platform identifiers currently found in the archive are `A`, `B`, and `C`. Standard
imagery uses operational mode `INS-NOBS`; calibration-oriented modes `INS-RAW` and
`INS-VIC` are also accepted by the query.

The CLI choices and descriptions follow the primary [ESA Sentinel-2 product
documentation](https://sentiwiki.copernicus.eu/web/s2-products), [ESA Sentinel-2
mission description](https://sentiwiki.copernicus.eu/web/s2-mission), and [CDSE OData
documentation](https://documentation.dataspace.copernicus.eu/APIs/OData.html).

CDSE query manifests, a deduplicated scene catalog, and remote-asset metadata are
cached in `~/.sentinel-py/cache/cdse` by default. The query and download commands use
the same location, so `--cache-dir` can normally be omitted from both. Local download
state remains beside the downloaded data because it describes files at that exact
storage location. Set `SENTINEL_PY_HOME` to move all default reusable caches and logs,
or pass `--cache-dir` for a command-specific override.

Preprocess a spatial and seasonal subset of the local Level-2A cache by defining a
strict, versioned pipeline YAML and running it with:

```bash
sentinel-py run examples/s2_preprocess_pipeline.yaml --validate-only
sentinel-py run examples/s2_preprocess_pipeline.yaml
```

Paths inside the YAML are resolved relative to the YAML file. Nodes form a validated
directed acyclic graph: each registered processor owns its Pydantic configuration,
and named `inputs` refer to upstream node IDs. Named outputs use `from` to select the
node to materialize. The `s2.preprocess` processor returns an xarray Dataset backed
by Dask. An explicit projected metre CRS, scalar or x/y
resolution, pixel anchor, and y/x chunk shape define aligned bounds and chunk windows.
Each scene/chunk task directly reads all requested bands and SCL, applies XML NODATA
and SATURATED masks before BOA offsets and resampling, aligns to the canonical grid,
and applies the SCL mask. Reflectance DN remains `uint16`; SCL is `uint8`.

Building this Dataset performs SAFE discovery and reads XML metadata, but raster I/O
remains deferred until an output writer requests Dask computation. The example's
`format: cog` writer creates compressed, internally tiled COGs for each scene,
variable, and canonical spatial chunk. It writes files atomically and records
resumable state under the output directory. Use `format: xarray` only when the caller
needs the lazy in-memory artifact without materialization.

All requested outputs are combined into one Dask computation so they reuse common
upstream reads. Local execution uses isolated Dask worker processes; external Dask
and Slurm submit the same graph to their configured clusters. Native raster calls
are serialized between threads within each worker process. Workers return structured
results while logging and state updates remain centralized. See
`examples/s2_preprocess_pipeline.yaml` and
`docs/architecture/processing-pipeline.md` for the complete configuration and
contracts.

On a Slurm system, replace the example's `execution` section with site-specific
resources such as:

```yaml
execution:
  method: slurm
  jobs: 16
  cores_per_job: 8
  processes_per_job: 8
  memory_per_job: 64GiB
  walltime: "04:00:00"
  queue: regular
  account: my-project
  local_directory: /path/to/node/or/scratch/storage
  job_script_prologue:
    - source /path/to/sentinel-py/.venv/bin/activate
```

To use a Dask cluster started outside sentinel-py, select `method: dask` and set
`scheduler_address`. Workers must see the source data, output directory, and the
same sentinel-py software environment.

### Downloading Sentinel-1 from ASF

Query ASF and save the results as a reusable manifest:

```bash
sentinel-py asf query \
  --aoi data/aois/toolik_025_aoi.geojson \
  --years "2023 2024" \
  --speriod 06-01 \
  --eperiod 08-31 \
  --crs EPSG:4326 \
  --product-levels GRD_HD \
  --beam-mode IW
```

The AOI can be any vector format readable by GeoPandas, including GeoJSON,
shapefiles, and GeoPackage files. AOIs with CRS metadata are reprojected to
EPSG:4326 for ASF; `--crs` supplies the CRS only when that metadata is absent.

By default, ASF queries use beam mode `IW`, dual-polarized `VV+VH` products, and
product level `GRD_HD`. Both flight directions are retained by default. Pass
`--flight-direction ASCENDING`, `--flight-direction DESCENDING`, or
`--flight-direction predominant` to restrict the manifest. For large-area processing,
retain both directions and group products by direction and relative orbit later.

The query help lists only Sentinel-1 choices, rather than the generic `asf_search`
constants shared by several SAR missions. ASF's documented Sentinel-1 data-product
levels are `GRD_HD`, `GRD_HS`, `GRD_MD`, `GRD_MS`, `GRD_FD`, `SLC`, `RAW`, and
`OCN`; metadata-only variants are also accepted. In the GRD codes, `H`, `M`, and `F`
mean high, medium, and full resolution, while `D` and `S` mean dual and single
polarization. `SLC` retains complex amplitude and phase; GRD contains detected,
multilooked ground-range data. Actual product availability depends on acquisition
mode and archive history. See the primary [ASF Search API keyword
reference](https://docs.asf.alaska.edu/api/keywords/) and [ESA Sentinel-1 product
description](https://sentiwiki.copernicus.eu/web/s1-products).

Sentinel-1 beam choices exposed by ASF are `IW`, `EW`, `WV`, and Stripmap beams
`S1`-`S6`. Standard polarization choices are `VV+VH`, `HH+HV`, `VV`, and `HH`;
ASF's additional `DUAL` catalog labels are also accepted. See the [ESA Sentinel-1
mission and acquisition-mode reference](https://sentiwiki.copernicus.eu/web/s1-mission).

If `--max-results` is set and a yearly window reaches that limit, the command warns
that its manifest may be truncated. Rerun with a higher limit or without the option
before using that manifest in a complete production workflow.

Identical ASF queries are cached by all spatial, temporal, and product filters. The
default cache is `~/.sentinel-py/cache/asf`. A cache hit recreates the query result
without contacting ASF. The download command uses the most recently queried cached
manifest automatically. For reproducible runs, pass the exact `manifest.parquet`
using `--query`; use `--cache-dir` on both commands to share a different cache
location.

ASF downloads require a NASA Earthdata Login account. Create a netrc-format
credentials file:

```bash
cat > "$HOME/.earthdata.netrc" <<'EOF'
machine urs.earthdata.nasa.gov
    login YOUR_EARTHDATA_USERNAME
    password YOUR_EARTHDATA_PASSWORD
EOF
chmod 600 "$HOME/.earthdata.netrc"
```

Download every unique URL in the manifest:

```bash
sentinel-py asf download \
  --outdir data/s1/raw \
  --config "$HOME/.earthdata.netrc" \
  --query "$HOME/.sentinel-py/cache/asf/QUERY_KEY/manifest.parquet" \
  --processes 4 \
  --retries 3
```

Before transferring files, both ASF and CDSE download commands print the selected
manifest, resolved asset count, known final dataset size, known additional storage
needed after accounting for valid local files, and the number of assets with unknown
sizes. They then use a standard `Continue with download? [y/N]` confirmation. CDSE
resolves and caches the requested S3 asset metadata before displaying this summary;
that preflight does not download image data. Pass `--yes` (or `-y`) only for an
intentional noninteractive run, such as a batch script:

```bash
sentinel-py asf download ... --yes
sentinel-py cdse download ... --yes
```

The credentials path is required for every ASF download command, matching CDSE's
required `--config` workflow.

ASF download state is stored at
`<outdir>/.sentinel-py/asf_downloads.parquet`. Existing files are checked against the
exact byte size and, when supplied by ASF, the MD5 checksum before they are skipped.
Downloads are first written to hidden `.part` files and atomically moved into place
only after validation, so an interrupted or corrupt download cannot replace a valid
product. Temporary `.part` files are removed after failures and Ctrl-C. A live
progress bar reports completed products and running downloaded, skipped, and failed
counts. Transient DNS, connection, timeout, HTTP 429/5xx, truncated-download, and
checksum failures are retried with exponential backoff. The command exits nonzero if
any required product still fails.
