import geopandas as gpd

src = "/mnt/poseidon/remotesensing/6ru/sentinel-py/data/aois/bioclimate_latlon/bioclimate_latlon.shp"
dst = "/mnt/poseidon/remotesensing/6ru/sentinel-py/data/aois/bioclimate_latlon/bioclimate_latlon_noglacier.shp"

bioclim = gpd.read_file(src)
print(f"Source: {len(bioclim)} features, zones: {sorted(bioclim['zone'].unique())}")

bioclim_veg = bioclim[bioclim["zone"] != 0].reset_index(drop=True)
print(
    f"Filtered: {len(bioclim_veg)} features, zones: {sorted(bioclim_veg['zone'].unique())}"
)

bioclim_veg.to_file(dst)
print(f"Saved -> {dst}")
