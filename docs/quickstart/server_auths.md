# Downloading with Authentication

## Search and Download Options

### ASF asf_search

For users in the United States, the National Aeronautics and Space Administration (NASA) Alaska Satellite Facility (ASF) hosts Sentinel-1 data in Fairbanks, Alaska. If you are in the U.S. and want to download Sentinel-1 data, we recommend using the ASF servers instead of the ESA servers. Sentinel-py wraps around the ASF-developed (`asf_search`)[https://docs.asf.alaska.edu/asf_search/basics/] Python package, which searches the Earthdata catalog and downloads Sentinel-1 data, which is managed by ASF DAAC and stored on servers in Fairbanks, Alaska. To use the `sentinel-py asf search` and `sentinel-py asf download` commands, you will need to create an Earthdata account and set up your `.netrc` file. See the [ASF Search documentation](https://docs.asf.alaska.edu/asf_search/basics/) for more information.

#### Set up your `.netrc` file

First, you will need to [create a free earthdata account](https://urs.earthdata.nasa.gov/users/new). Then, create a `.netrc` file in your home directory (i.e., `~/.netrc`) and add the following lines, replacing `your_username` and `your_password` with your Earthdata credentials:

```
machine urs.earthdata.nasa.gov
login your_username
password your_password
```

### ESA phidown

Sentinel-py currently wraps around the European Space Agency (ESA)-developed (`phidown`)[https://github.com/ESA-PhiLab/phidown], which is a Python package and CLI for searching and downloading Copernicus Data Space Ecosystem (CDSE) products and PhiSat-2 INSULA platform files. We use their Python API inside our CLI, which currently only supports searching and downloading Sentinel-2 products. To download other products, we recommend using `phidown` directly. The AWS S3 server is physically located in Warsaw Poland, so if you are downloading large amounts of data from outside Europe, you will experience slower download speeds. To use the `sentinel-py cdse search` and `sentinel-py cdse download` commands, you will need to create a CDSE account and set up your `.s5cfg` file (see the [phidown documentation](https://esa-philab.github.io/phidown/getting_started.html#prerequisites) for more information).

#### Set up your `.s5cfg` file

First, you will need to [create a CDSE account](https://dataspace.copernicus.eu/). Then, you will need to [generate AWS S3 credentials](https://eodata-s3keysmanager.dataspace.copernicus.eu/panel/s3-credentials) for your CDSE account. Finally, create a `.s5cfg` file in your home directory (i.e., `~/.s5cfg`) and add the following lines, replacing `your_access_key` and `your_secret_key` with your CDSE credentials.

```
[default]
aws_access_key_id = your_access_key
aws_secret_access_key = your_secret_key
aws_region = eu-central-1
host_base = eodata.dataspace.copernicus.eu
host_bucket = eodata.dataspace.copernicus.eu
use_https = true
check_ssl_certificate = true
```