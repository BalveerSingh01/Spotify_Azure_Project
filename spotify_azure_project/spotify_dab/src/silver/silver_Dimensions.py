# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


import os
import sys
project_path = os.path.join(os.getcwd(),'..','..')

sys.path.append(project_path)


# COMMAND ----------

# MAGIC %md
# MAGIC ##DimUsers

# COMMAND ----------

# MAGIC %md
# MAGIC ###AutoLoader

# COMMAND ----------

df_user = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
        .option("cloudFiles.schemaLocation" , "abfss://silver@storageminee.dfs.core.windows.net/DimUser/checkpoint")\
            .option("SchemaEvolutionMode","rescue")\
                .load("abfss://bronze@storageminee.dfs.core.windows.net/DimUser")


# COMMAND ----------

display(df_user,checkpointLocation ="abfss://silver@storageminee.dfs.core.windows.net/DimUser/checkpoints")

# COMMAND ----------




# COMMAND ----------

# df_user = (spark.readStream.format("cloudFiles")
#     .option("cloudFiles.format", "parquet")
#     .option("cloudFiles.schemaLocation", "abfss://silver@storageminee.dfs.core.windows.net/DimUser/_schema")
#     .option("cloudFiles.schemaEvolutionMode", "rescue")
#     .load("abfss://bronze@storageminee.dfs.core.windows.net/DimUser"))


# COMMAND ----------

# display(df_user, checkpointLocation="abfss://silver@storageminee.dfs.core.windows.net/DimUser/_preview_checkpoint1")

# COMMAND ----------

df = df_user.withColumn("user_name",upper(col("user_name")))
df = df.withColumn("user_demo",lit(None))

# COMMAND ----------

# df_preview = spark.read.format("parquet") \
#     .load("abfss://bronze@storageminee.dfs.core.windows.net/DimUser")

# display(df_preview)

# COMMAND ----------

# df = df_preview.withColumn("user_name",upper(col("user_name")))
# df = df.withColumn("user_demo",lit(None))

# COMMAND ----------

display(df)

# COMMAND ----------

from utlis.Transformations import reusable

# COMMAND ----------

df_user_obj = reusable()
df = df_user_obj.dropColumns(df,['user_demo','_rescued_data'])
df = df.dropDuplicates(['user_id'])


# COMMAND ----------

df.columns

# COMMAND ----------

df.writeStream.format('delta')\
    .outputMode('append')\
        .option("checkpointLocation","abfss://silver@storageminee.dfs.core.windows.net/DimUser/checkpointv2")\
            .trigger(once=True)\
                .option("path","abfss://silver@storageminee.dfs.core.windows.net/DimUser/data")\
                .toTable("spotify_cata.silver.DimUser")


# COMMAND ----------

# MAGIC %md
# MAGIC ###DimArtist

# COMMAND ----------

df_art = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
        .option("cloudFiles.schemaLocation","abfss://silver@storageminee.dfs.core.windows.net/DimArtist/checkpoint")\
            .option("SchemaEvolutionMode","addNewColumns")\
                .load("abfss://bronze@storageminee.dfs.core.windows.net/DimArtist")

# COMMAND ----------

display(df_art,checkpointLocation = "abfss://silver@storageminee.dfs.core.windows.net/DimArtist/checkpoint")

# COMMAND ----------

df_art.columns

# COMMAND ----------

from utlis.Transformations import reusable

# COMMAND ----------

df_art_obj = reusable()
df_art = df_art_obj.dropColumns(df_art,['_rescued_data'])
df_art= df_art.dropDuplicates(['artist_id'])

# display(df_art)

# COMMAND ----------

df_art.columns

# COMMAND ----------

df_art.writeStream.format("delta")\
    .outputMode("append")\
        .option("checkpointLocation","abfss://silver@storageminee.dfs.core.windows.net/DimArtist/checkpointv2")\
            .trigger(once=True)\
                .start("abfss://silver@storageminee.dfs.core.windows.net/DimArtist/data")

# COMMAND ----------

# MAGIC %md
# MAGIC saving to catalog silver

# COMMAND ----------

df_art.writeStream.format("delta")\
    .outputMode("append")\
        .option("checkpointLocation","abfss://silver@storageminee.dfs.core.windows.net/DimArtist/checkpointv2")\
            .trigger(once=True)\
                .option("path","abfss://silver@storageminee.dfs.core.windows.net/DimArtist/data")\
                .toTable("spotify_cata.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC ##DimTrack

# COMMAND ----------

dim_track = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
        .option("cloudFiles.schemaLocation","abfss://silver@storageminee.dfs.core.windows.net/DimTrack/checkpoint")\
        .option("schemaEvolutionMode","addNewColumns")\
            .load("abfss://bronze@storageminee.dfs.core.windows.net/DimTrack")

# COMMAND ----------

display(dim_track,checkpointLocation = "abfss://silver@storageminee.dfs.core.windows.net/DimTrack/checkpoint")

# COMMAND ----------

df_track = dim_track.withColumn("durationflag",when(col("duration_sec")<150,"Low")\
    .when(col("duration_sec")<300,"Medium")\
        .otherwise("High"))

df_track = df_track.withColumn("track_name",regexp_replace(col('track_name'),"-"," "))
df_track = reusable().dropColumns(df_track,['_rescued_data'])

# COMMAND ----------

df_track.writeStream.format("delta")\
    .outputMode("append")\
        .option("checkpointLocation","abfss://silver@storageminee.dfs.core.windows.net/DimTrack/checkpointv2")\
            .trigger(once=True)\
                .option("path","abfss://silver@storageminee.dfs.core.windows.net/DimTrack/data")\
                .toTable("spotify_cata.silver.DimTrack")


# COMMAND ----------

# MAGIC %md
# MAGIC ##DimDate

# COMMAND ----------

dim_date= spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
        .option("cloudFiles.schemaLocation","abfss://silver@storageminee.dfs.core.windows.net/DimDate/checkpoint")\
            .option("schemaEvolutionMode","addNewColumns")\
                .load("abfss://bronze@storageminee.dfs.core.windows.net/DimDate")

# COMMAND ----------

display(dim_date,checkpointLocation = "abfss://silver@storageminee.dfs.core.windows.net/DimDate/checkpoint")

# COMMAND ----------

dim_date = reusable().dropColumns(dim_date,["_rescued"])

# COMMAND ----------

dim_date.writeStream.format('delta')\
    .outputMode("append")\
        .option("checkpointLocation","abfss://silver@storageminee.dfs.core.windows.net/DimDate/checkpointv2")\
            .trigger(once = True)\
                .option("path","abfss://silver@storageminee.dfs.core.windows.net/DimDate/date")\
                    .toTable("spotify_cata.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC ##FactStream

# COMMAND ----------

df_fact= spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
        .option("cloudFiles.schemaLocation","abfss://silver@storageminee.dfs.core.windows.net/FactStream/checkpoint")\
            .option("schemaEvolutionMode","addNewColumns")\
                .load("abfss://bronze@storageminee.dfs.core.windows.net/FactStream")

# COMMAND ----------

display(df_fact,checkpointLocation = "abfss://silver@storageminee.dfs.core.windows.net/FactStream/checkpoint")

# COMMAND ----------

df_fact = reusable().dropColumns(df_fact,["_rescued_data"])

# COMMAND ----------

df_fact.columns

# COMMAND ----------

df_fact.writeStream.format("delta")\
    .outputMode("append")\
        .option("checkpointLocation","abfss://silver@storageminee.dfs.core.windows.net/FactStream/checkpointv2")\
            .trigger(once=True)\
                .option("path","abfss://silver@storageminee.dfs.core.windows.net/FactStream/data")\
                    .toTable("spotify_cata.silver.FactStream")

# COMMAND ----------

df_fact.explain()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE STORAGE CREDENTIAL credentail;
# MAGIC
# MAGIC databricks metastores update --id f23cfa1c-090b-409b-b686-b27d9966b65a --storage-root-credential-id 802d8172-ea52-4b53-82e5-a12a4b38f375
# MAGIC

# COMMAND ----------

