### Requirements

- Python 3.13+
- [uv 0.11.7+](https://docs.astral.sh/uv/getting-started/installation/)

Querying and downloading data do not require the Python GDAL bindings or SNAP.
Sentinel-2 raster processing requires GDAL 3.11.5 or newer; install it with the
optional `processing` dependency described below. The Python bindings must be built
against the same version of the native GDAL library available in the environment.

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

GDAL's Python package compiles against a native GDAL installation. If this command
cannot find `gdal-config`, or reports a version mismatch, install/load a native GDAL
3.11.5+ build first and ensure its `gdal-config` is on `PATH`. This does not affect
the default query/download installation.

### NERSC (Perlmutter)

For query and download work on Perlmutter, no GDAL module or GDAL Python binding is
needed. After cloning the repository and installing
[uv](https://docs.astral.sh/uv/getting-started/installation/) in your user account:

```bash
cd sentinel-py
uv sync --no-dev
uv run sentinel-py --help
```

The first command creates `.venv` and installs only the base query/download
dependencies. Do not pass `--extra processing` unless the job will run the
GDAL-based Sentinel-2 processing commands.

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
