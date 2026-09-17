from pyspark import pipelines as dp

def dimuser_flow():
    return spark.readStream.table(
        "spotify_cata.silver.dimtrack"
    )


dp.create_streaming_table(
    name="spotify_cata.gold.dimtrack"
    )


dp.create_auto_cdc_flow(
    target = "spotify_cata.gold.dimtrack",
    source = "spotify_cata.silver.dimtrack",
    keys= ["track_id"],
    sequence_by = "updated_at",
    stored_as_scd_type = 2
    )







