from pyspark import pipelines as dp

def dimuser_flow():
    return spark.readStream.table(
        "spotify_cata.silver.factstream"
    )


dp.create_streaming_table(
    name="spotify_cata.gold.factstream"
    )


dp.create_auto_cdc_flow(
    target = "spotify_cata.gold.factstream",
    source = "spotify_cata.silver.factstream",
    keys= ["stream_id"],
    sequence_by = "stream_timestamp",
    stored_as_scd_type = 2
    )







