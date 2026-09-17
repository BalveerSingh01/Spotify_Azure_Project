from pyspark import pipelines as dp

def dimuser_flow():
    return spark.readStream.table(
        "spotify_cata.silver.dimartist"
    )


dp.create_streaming_table(
    name="spotify_cata.gold.dimartist"
    )


dp.create_auto_cdc_flow(
    target = "spotify_cata.gold.dimartist",
    source = "spotify_cata.silver.dimartist",
    keys= ["artist_id"],
    sequence_by = "updated_at",
    stored_as_scd_type = 2
    )







