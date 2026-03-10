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
uv pip install -e .
```

3. Set up the SNAP environment:
```bash
export SNAP_HOME=/path/to/snap/bin
export PYTHONPATH=$SNAP_HOME/snap-python:$PYTHONPATH
```

### Downloading S2
To download Sentinel-2 data, you will need to have an account on the [Copernicus Open Access Hub](https://scihub.copernicus.eu/dhus/#/home) and obtain your credentials. Once you have your credentials, you can use the CLI to download data. We recommend exporting your credentials as environment variables for convenience:
```bash
export CDSE_USERNAME=your_username
echo <your_password> > $HOME/.cdse/cdse_pw
chmod 600 $HOME/.cdse/cdse_pw
export CDSE_PASSWORD_FILE=$HOME/.cdse/cdse_pw
```