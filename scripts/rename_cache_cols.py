import pandas as pd

df = pd.read_parquet(
    "/mnt/poseidon/remotesensing/6ru/sentinel-py/examples/cache/all_downloaded_images.parquet"
)
df = df.rename(
    columns={
        "Name": "safedir",
        "S3Path": "s3_path",
        "band": "band_name",
        "resolution": "resolution_m",
        "rel_path": "img_path_in_safedir",
        "expected_size": "s3_expected_size",
    }
)
df["local_actual_size"] = None
df.to_parquet(
    "/mnt/poseidon/remotesensing/6ru/sentinel-py/examples/cache/all_downloaded_images.parquet",
    index=False,
)
