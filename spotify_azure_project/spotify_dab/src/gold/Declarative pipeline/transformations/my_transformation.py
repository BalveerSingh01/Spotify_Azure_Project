from pyspark import pipelines as dp

def dimuser_flow():
    return spark.readStream.table(
        "spotify_cata.silver.dimuser"
    )


dp.create_streaming_table(
    name="spotify_cata.gold.dimuser"
    )


dp.create_auto_cdc_flow(
    target = "spotify_cata.gold.dimuser",
    source = "spotify_cata.silver.dimuser",
    keys= ["user_id"],
    sequence_by = "updated_at",
    stored_as_scd_type = 2
    )







