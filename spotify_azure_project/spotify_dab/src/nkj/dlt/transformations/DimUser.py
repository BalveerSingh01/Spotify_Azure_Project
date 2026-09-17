from pyspark import pipelines as dp

@dp.table(
    name="spotify_cata.gold.dimuser"
)
def dimuser():
    return spark.readStream.table(
        "spotify_cata.silver.dimuser"
    )