# Databricks notebook source
# MAGIC %sql
# MAGIC select * from spotify_cata.silver.dimuser

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
        .option("cloudFiles.schemaLocation","abfss://silver@storageminee.dfs.core.windows.net/DimUser/checkpoint_nw")\
            .option("schemaEvolutionMode","rescued")\
                .load("abfss://bronze@storageminee.dfs.core.windows.net/DimUser")




# COMMAND ----------

display(df,checkpointLocation = "abfss://silver@storageminee.dfs.core.windows.net/DimUser/checkpoint_nw")

# COMMAND ----------

df1= spark.read.format('delta')\
    .load("abfss://silver@storageminee.dfs.core.windows.net/DimUser/data")
df1.show(3)

# COMMAND ----------

# df1 was write  in DimUsepract/prac


user_updates= spark.read.format('delta')\
    .load("abfss://silver@storageminee.dfs.core.windows.net/DimUsepract/prac")
user_updates.count()


# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

useres= user_updates.withColumn("country",when(col("country")=='Uruguay','ur').otherwise(col("country")))


useres.filter(col("country")=="ur").show()



# COMMAND ----------

useres.write.format('delta').mode('append')\
    .save("abfss://silver@storageminee.dfs.core.windows.net/DimUsepract/prac")

# COMMAND ----------

df2= spark.read.format('delta')\
    .load("abfss://silver@storageminee.dfs.core.windows.net/DimUsepract/prac")

df2.filter(col("user_id")==150).show()


# COMMAND ----------

from delta.tables import DeltaTable
target=DeltaTable.forPath(spark,"abfss://silver@storageminee.dfs.core.windows.net/DimUser/data")

# COMMAND ----------

target.alias('t').merge(
    df2.alias('s'),"t.user_id = s.user_id"
).whenMatchedUpdate(
    set = {
        't.country':'s.country',
        't.subscription_type':'s.subscription_type'
    }
).whenNotMatchedInsertAll().execute()


# COMMAND ----------


df2.createOrReplaceTempView("updates")

# COMMAND ----------

# MAGIC %sql
# MAGIC --find which records needs to update
# MAGIC
# MAGIC -- select s.*
# MAGIC -- from updates s
# MAGIC -- left join spotify_cata.silver.dimuser t
# MAGIC -- on t.user_id = s.user_id
# MAGIC -- where t.country <> s.country
# MAGIC
# MAGIC --duplicates
# MAGIC
# MAGIC -- select user_id
# MAGIC -- from updates
# MAGIC -- group by user_id
# MAGIC -- having count(*)>1
# MAGIC -- order by user_id
# MAGIC

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

window_spec = Window.partitionBy("user_id").orderBy(col("updated_at").desc())

df3= df2.withColumn("rnk",row_number().over(window_spec)).filter(col('rnk')==1).drop(col("rnk"))

display(df3)

# COMMAND ----------

target.alias('t').merge(
    df3.alias('s'),"t.user_id = s.user_id"
).whenMatchedUpdate(
    set = {
        't.country':'s.country',
        't.subscription_type':'s.subscription_type'
    }
).whenNotMatchedInsertAll().execute()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df2= spark.read.format('delta')\
    .load("abfss://silver@storageminee.dfs.core.windows.net/DimUser/data")

# COMMAND ----------

source= df2.withColumn("country",when(col("country")=='Chile','Switzerland').otherwise(col("country")))

# df2.filter(col("country")=="Chile").show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ##SCD Type2

# COMMAND ----------

dimuser = spark.read.table("spotify_cata.gold.dimuser")
dimuser= dimuser.withColumn("isActive",lit('true')).drop("__END_AT","__START_AT")

dimuser.write.format("delta").saveAsTable("spotify_cata.silver.dimuse_SCDType_2")

# COMMAND ----------


dimuser = spark.read.table("spotify_cata.silver.dimuse_SCDType_2")
dimuser.show(5)

# COMMAND ----------

df.createOrReplaceTempView('source')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- updated_records
# MAGIC
# MAGIC select s.*
# MAGIC from source s
# MAGIC left join spotify_cata.silver.dimuse_SCDType_2 t
# MAGIC on t.user_id = s.user_id
# MAGIC where s.country <> t.country
# MAGIC
# MAGIC -- new recorsd
# MAGIC -- select s.*
# MAGIC -- from source s
# MAGIC -- left join spotify_cata.silver.dimuse_SCDType_2 t
# MAGIC -- on t.user_id = s.user_id
# MAGIC -- where t.user_id is null

# COMMAND ----------

current_rows = dimuser.filter(col("isActive")=='true')
current_rows.count()

# COMMAND ----------

from delta.tables import DeltaTable
target = DeltaTable.forName(spark, 'spotify_cata.silver.dimuse_SCDType_2')

# COMMAND ----------

target.alias("t").merge(
    source.alias("s"),"t.user_id = s.user_id AND t.isActive = 'true'"
).whenMatchedUpdate(
    condition="""
    t.country <> s.country
    or
    t.subscription_type <> s.subscription_type
    """,
    set= {
        "t.isActive":"false",
        "t.end_date":"current_date()"
    }
).execute()

# COMMAND ----------

.filter((col("s.country") != col("c.country")) |(col("s.subscription_type") != col("c.subscription_type")))


# COMMAND ----------

updated_rows = source.alias("s").join(current_rows.alias("c"),col("c.user_id")==col("s.user_id"),"inner")\
    .filter((col("s.country")!= col("c.country")) | (col("s.subscription_type")!= col("c.subscription_type")))

    

updated_rows= updated_rows.select()\
    .withColumn("start_date",lit(current_date()))\
        .withColumn("end_date",lit(None))\
            .withColumn("isActive",lit('true'))

updated_rows.write.mode("append").saveAsTable("spotify_cata.silver.dimuse_SCDType_2")


new_rows = source.alias("s").join(
    current_rows.alias("c"),col("c.user_id")==col("s.user_id"),"left_anti")

new_rows= new_rows.select()\
    .withColumn("start_date",lit(current_date()))\
        .withColumn("end_date",lit(None))\
            .withColumn("isActive",lit('true'))

new_rows.write.mode("append").saveAsTable("spotify_cata.silver.dimuse_SCDType_2")




# COMMAND ----------

# MAGIC %md
# MAGIC ##Schema Evolution

# COMMAND ----------

new_schema = spark.read.format('delta')\
    .load("abfss://silver@storageminee.dfs.core.windows.net/DimUser/data")


new_schema = new_schema.withColumn("new_column",lit(None))
new_schema1 = new_schema.withColumn("user_id",col("user_id").cast("string"))


# COMMAND ----------

new_schema.write.mode("append").saveAsTable("spotify_cata.silver.dimuse_SCDType_2")

# COMMAND ----------

new_schema1.write.mode("append").saveAsTable("spotify_cata.silver.dimuse_SCDType_2")

# COMMAND ----------

new_schema.write.mode("append").option("mergeSchema","true").saveAsTable("spotify_cata.silver.dimuse_SCDType_2")


# COMMAND ----------

# MAGIC %md
# MAGIC ##Slow Spark Job Optimization

# COMMAND ----------

new = spark.read.format('delta')\
    .load("abfss://silver@storageminee.dfs.core.windows.net/FactStream/data")


from pyspark.sql.functions import lit, explode, sequence

df = new

# Create multiplier
multiplier = spark.range(200000)

# Expand rows
factstream_df = df.crossJoin(multiplier)

# COMMAND ----------

factstream_df.write.format("delta").save("abfss://silver@storageminee.dfs.core.windows.net/FactStrea_large_prac/data")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE spotify_cata.silver.factstream_large
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@storageminee.dfs.core.windows.net/FactStrea_large_prac/data"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE spotify_cata.silver.factstream_large

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE spotify_cata.silver.factstream_large
# MAGIC ZORDER BY (user_id)

# COMMAND ----------

large_df.show(5)

# COMMAND ----------

dimuser= spark.read.format('delta')\
    .load("abfss://silver@storageminee.dfs.core.windows.net/DimUser/data")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

join = dimuser.alias('s').join(large_df.alias('l'),col("s.user_id")==col("l.user_id"),"inner")
join.count()

# COMMAND ----------

join.explain()

# COMMAND ----------

broadcast_join = large_df.alias('l').join(broadcast(dimuser.alias('s')),col("s.user_id")==col("l.user_id"),"inner")
broadcast_join.count()

# COMMAND ----------

broadcast_join.explain()

# COMMAND ----------

large_df = large_df.withColumn("salt",floor(rand()*10))

# COMMAND ----------

large_df.show(100)

# COMMAND ----------

dimuser = dimuser.withColumn("salt",explode(sequence(lit(0),lit(9))))

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled","true")

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.skewJoin.enalbed","true")

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.coelscePartitions.enabled","true")

# COMMAND ----------



# COMMAND ----------

