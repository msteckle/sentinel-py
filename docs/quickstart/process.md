# Processing Data

Raw remote sensing data is usually pre-processed to correct for sensor and atmospheric effects, then once it is ready to use, it can be analyzed and processed to extract meaningful information. For Sentinel-2 bottom-of-atmosphere (BOA) data, though atmospheric corrections have already been applied, the BOA data have to be harmonized of time by applying offsets to the pixel digital numbers (DNs) based on the processing baseline. If we don't do this, we can't accurately compare or combine data from different acquisition times. Sentinel provides a Scene Classification Layer (SCL) band that contains bitwise information about the type of surface or object each pixel represents, such as vegetation, water, clouds, or shadows. Often, we can use this band to mask out pixels containing unwanted information, particularly cloud cover. Once corrections have been applied and pixels have been masked, the data then need to be further processed for analysis, such as calculating vegetation indices, smoothing time series, applying machine learning algorithms, or performing other types of geospatial analysis.

Rather than creating one ``sentinel-py`` command for each processing step, we created ``sentinel-py run``, which accepts a ``YAML`` configuration file. In the configuration file, you can specify the sequence of processing steps, the input data, and the parameters for each processor. This approach allows for a more streamlined and reproducible workflow, as all processing steps are defined in a single file and can be executed with a single command. This also prevents reading and writing intermediate files multiple times, reducing I/O overhead and improving overall efficiency. A processing step can be any operation defined by a processor, such as pre-processing, masking, or calculating indices, and can be easily reused across different pipelines. A processor can do one action or multiple actions depending on its design.

For example, ``sentinel-py`` has a ``s2.preprocess`` processor that handles the pre-processing of Sentinel-2 data, which specifically includes harmonizing the BOA data over time by applying the BOA offsets, plus masking unwanted pixels based on the Scene Classification Layer (SCL) band. This processor can be configured in the YAML file with parameters such as the input data source, the spectral bands to use, the mask classes to apply, and the nodata value. By defining these parameters in the configuration file, users can easily customize the pre-processing workflow to suit their specific needs.

To see what processors are available, you can check ``sentinel-py/pipeline/processors/``. This directory contains all the built-in processors that come with the library, and you can explore their implementations to understand how they work and how to use them in your YAML configuration files. To use one in your ``YAML``, you would reference its ``type_name`` under the ``nodes`` section and provide the necessary configuration parameters. For example:

```yaml
sources:
  s2:
    type: s2.l2a.local
    data_dir: ../data/s2/raw

nodes:
  - id: preprocess
    type: s2.preprocess
    source: s2
    bands: [B02, B03, B04, B05, B06, B07, B08, B8A, B11, B12]
    mask_classes: [0, 1, 3, 8, 9, 10, 11]
    nodata: 65535
```