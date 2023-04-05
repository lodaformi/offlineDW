create external table if not exists dim.dim_area_dict(
    geohash string,
    province string,
    city string,
    region string
)
stored as parquet
;