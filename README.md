### Requirements
- Python 3.7+
- [SNAP 9.0+](https://step.esa.int/main/download/snap-download/previous-versions/)
- [GDAL 3.4.3](https://gdal.org/en/stable/download_past.html)
- [uv 0.7.6+](https://docs.astral.sh/uv/getting-started/installation/#pypi)

### Installation
1. Clone the repository:
```bash
git clone https://github.com/msteckle/sentinel-py.git
cd sentinel-py
```
2. Install the required Python packages with uv:
```bash
uv sync
```
3. Set up the SNAP environment:
```bash
export SNAP_HOME=/path/to/snap/bin
export PYTHONPATH=$SNAP_HOME/snap-python:$PYTHONPATH
```