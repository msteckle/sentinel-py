# Downloading on a Slurm cluster (CADES Pathfinder)

Large downloads must not run on a login node. Login nodes are shared, and CADES forbids launching parallel or threaded jobs on them. The downloaders use thread pools and spawn several `s5cmd` processes at once, so a long run on a login node is typically killed (the shell prints `Killed` with no Python error). Run downloads on a compute node through `sbatch` instead.

## Check that compute nodes can reach each provider

Run these from a login node. Each `srun` executes on a compute node. A 403 or 405 response is fine and only means the server rejected an unauthenticated HEAD request. A `000` result or a timeout means the host is blocked.

```bash
srun -p serial -q normal -N1 -n1 -c 1 --mem=1g -t 00:05:00 bash -c '
for u in \
  https://eodata.dataspace.copernicus.eu \
  https://urs.earthdata.nasa.gov \
  https://datapool.asf.alaska.edu \
  https://stac.pgc.umn.edu/api/v1/ \
  https://pgc-opendata-dems.s3.us-west-2.amazonaws.com \
  https://step.esa.int/auxdata/dem/egm96/ww15mgh_b.zip \
  https://s1qc.asf.alaska.edu/aux_poeorb/ ; do
  printf "%s  " "$u"; curl -sI -m 60 -o /dev/null -w "%{http_code}\n" "$u" || echo FAIL
done'
```

In testing on Pathfinder, every host above was reachable from compute nodes except `s1qc.asf.alaska.edu`, which times out there but answers on the login node. Sentinel-1 orbit files must therefore be staged from a login node (see below).

## Required Slurm flags

Pathfinder rejects jobs that omit any of these, with one error per missing flag: `-p` (partition), `-q` (QOS), `-c` (CPUs per task) and `--mem` (memory). Downloads are single-node jobs, so use the `serial` partition and size `-c` to your parallelism (for CDSE, `--parallel-scenes` x `--parallel-bands`).

## Example: Sentinel-2 from CDSE

Save as `cdse_dl.sbatch`, adjust the environment activation and paths, and submit with `sbatch cdse_dl.sbatch`.

```bash
#!/bin/bash
#SBATCH -p serial
#SBATCH -q normal
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 8
#SBATCH --mem=16g
#SBATCH -t 24:00:00
#SBATCH -J cdse_dl
#SBATCH -o %x_%j.out

source /path/to/venv/bin/activate      # compute nodes do not inherit your login PATH setup
which sentinel-py s5cmd || exit 1

sentinel-py cdse download --mission S2 \
  --bands "B02 B03 B04 B05 B06 B07 B08 B8A B11 B12 SCL" \
  --outdir "$OUTPATH/s2/raw" --res 20 \
  --config "$HOME/.s5cfg" --log "$LOGPATH/cdse.log" \
  --parallel-scenes 2 --parallel-bands 4 --yes
```

Pass `--yes` so the batch job does not stall at the storage-estimate confirmation prompt. Check the partition walltime limit with `sinfo -p serial` and lower `-t` to fit. Downloads are resumable: completed files are skipped, so when a job reaches its limit, resubmit the same script.

## Sentinel-1 from ASF, and orbit files

`sentinel-py asf download` also stages Sentinel-1 orbit files and the EGM96 geoid grid for offline SNAP processing. Because `s1qc.asf.alaska.edu` is unreachable from compute nodes, split the work into two runs of the same command with the same manifest and output directory.

1. In an `sbatch` job on a compute node, download the scenes and skip orbit staging:

    ```bash
    sentinel-py asf download <your usual arguments> --orbits none --processes 4 --yes
    ```

2. Afterwards, on a login node, rerun the same command to stage orbit files and the EGM96 grid. Already-downloaded products are skipped, and one process keeps the load light:

    ```bash
    sentinel-py asf download <your usual arguments> --orbits precise-or-restituted --processes 1 --yes
    ```

Repeat step 2 periodically. Precise orbits are published a few weeks after acquisition, so recent scenes fall back to restituted orbits and are upgraded on a later rerun. If you prefer not to touch the login node at all, ask CADES support to allow `s1qc.asf.alaska.edu` from compute nodes.

## ArcticDEM from PGC

`https://stac.pgc.umn.edu` and the ArcticDEM S3 bucket were reachable from compute nodes, so `sentinel-py pgc` downloads can be submitted with the same `sbatch` pattern as above.

## Troubleshooting

- `sentinel-py: No such file or directory` on the node: activate the virtual environment inside the batch script, as in the example.
- `Killed` on a login node: move the run into `sbatch`.
- `no object found` from an `s5cmd ls` of a bare prefix: expected. It is an authenticated S3 reply that confirms credentials and connectivity work; list with a wildcard such as `s3://eodata/.../*` instead.
