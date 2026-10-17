# Installation

Querying and downloading Sentinel data only requires the base Python environment.
Sentinel-2 raster processing additionally requires the `processing` extra:

```bash
uv sync --extra processing
```

Rasterio supplies the production raster reader, reprojection operations, and COG
writer; no separate system-level raster build is required.

## Sentinel-1 processing (SNAP)

Sentinel-1 GRD preprocessing (orbit application, noise removal, calibration, terrain correction) runs the ESA SNAP `gpt` command-line tool once per scene. SNAP is a separate Java application, not a Python package, so it is installed outside `uv` and is only needed for Sentinel-1 processing; query, download, and Sentinel-2 workflows do not use it. Use SNAP 14.0.0 (released 2026-08-18), the Sentinel Toolboxes build that includes the Microwave toolbox. Uninstall older SNAP versions first, because ESA recommends one version per machine.

### macOS (Apple Silicon)

Download the installer and its checksum list, then compare checksums before opening the installer:

```bash
curl -fLO https://download.esa.int/step/snap/14.0/installers/esa-snap_sentinel_macos_arm-14.0.0.dmg
curl -fLO https://step.esa.int/downloads/14.0/installers/sha256sums
shasum -a 256 esa-snap_sentinel_macos_arm-14.0.0.dmg
grep esa-snap_sentinel_macos_arm-14.0.0.dmg sha256sums
```

The two hashes must be identical. Use `esa-snap_sentinel_macos_intel-14.0.0.dmg` on Intel Macs. Open the DMG, run the installer, and accept the default components (the Microwave toolbox is required; the SMOS toolbox is not). The installer bundles its own Java runtime, so no separate Java install is needed. The default install directory is `/Applications/esa-snap`; export it in your shell profile:

```bash
export SNAP_HOME="/Applications/esa-snap"
```

### Linux (workstation or cluster)

```bash
curl -fLO https://download.esa.int/step/snap/14.0/installers/esa-snap_sentinel_linux-14.0.0.sh
curl -fLO https://step.esa.int/downloads/14.0/installers/sha256sums
sha256sum esa-snap_sentinel_linux-14.0.0.sh
grep esa-snap_sentinel_linux-14.0.0.sh sha256sums
bash esa-snap_sentinel_linux-14.0.0.sh
```

The installer is an install4j script; on a headless node it can usually be run unattended with `-q -dir <install directory>`. Install to a shared filesystem path that every compute node can read, and set `SNAP_HOME` to that directory.

### CADES Pathfinder

On CADES, install SNAP on project storage, which is shared across nodes and not purged. Do not use scratch (Lustre), which is purged periodically and handles many small files poorly, or `$HOME`, which is small. This example installs next to the repository, so the install stays with the project and is not scattered across the shared project directory. Run it on a login node, which has internet access, and keep the roughly 1 GB installer out of the repository:

```bash
mkdir -p /tmp/$USER-snap && cd /tmp/$USER-snap
curl -fLO https://download.esa.int/step/snap/14.0/installers/esa-snap_sentinel_linux-14.0.0.sh
curl -fLO https://step.esa.int/downloads/14.0/installers/sha256sums
sha256sum esa-snap_sentinel_linux-14.0.0.sh
grep esa-snap_sentinel_linux-14.0.0.sh sha256sums

export SNAP_HOME=/projects/<project>/<shared-dir>/sentinel-py/.snap-install
bash esa-snap_sentinel_linux-14.0.0.sh -q -dir "$SNAP_HOME"
rm -rf /tmp/$USER-snap
```

The two hashes must be identical. Select the Microwave toolbox if the installer asks about components. To keep SNAP's per-user files out of `$HOME` while you verify the install, export `_JAVA_OPTIONS="-Dsnap.userdir=<repository>/.snap-userdir"` first, then follow the steps below. Pipeline runs do not use this directory, because each scene gets its own temporary SNAP user directory. Add `.snap-install/` and `.snap-userdir/` to `.gitignore`, exclude them from any rsync or deploy of the repository, and export `SNAP_HOME` inside your `sbatch` script, because compute nodes do not inherit your login environment. The pipeline example for Slurm is `examples/pavc_gridded_rf_points_2019_hpc_training.sbatch`.

### Verify the install

Do not rely on a bare `gpt` command: macOS ships an unrelated `/usr/sbin/gpt` (a disk partitioning tool), which shadows SNAP's `gpt` whenever SNAP is not first on `PATH`. Always call SNAP's tool by its full path:

```bash
"$SNAP_HOME/bin/gpt" -h
"$SNAP_HOME/bin/gpt" Terrain-Correction -h
```

`"$SNAP_HOME/bin/gpt" --diag` prints the release version, which must be `14.0.0`. The first command lists the available operators and must include `Apply-Orbit-File`, `Calibration`, `Terrain-Correction`, `Terrain-Flattening`, and `Remove-GRD-Border-Noise`. The second prints the operator's parameters for this exact version, which is the reference for writing graph XML. If an operator is missing, open SNAP once and install the Microwave toolbox from Tools > Plugins.

### Memory and offline use

`gpt` uses the JVM settings in `$SNAP_HOME/bin/gpt.vmoptions`. The default heap is small for GRD scenes; set `-Xmx` to roughly 16 GB for one scene at a time, and keep it below the memory you request per Slurm task. SNAP downloads orbit files and auxiliary data on demand, which fails on offline compute nodes. `sentinel-py asf download` stages Sentinel-1 orbit files next to your ZIPs for this reason, so run it on a node with network access before submitting jobs. Terrain correction also fetches the EGM96 geoid grid (`auxdata/dem/egm96/ww15mgh_b.zip` under the SNAP user directory) even when the external DEM is ellipsoidal, so that file must be present before an offline run.
