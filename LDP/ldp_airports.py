from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp
import dlt


@dp.temporary_view
def stg_airports():

    return (
        spark.readStream
        .format("delta")
        .load("/Volumes/flyproject/bronze/bronze_volume/airports/data/")
    )


@dp.temporary_view
def trans_airports():

    return (
        spark.readStream
        .table("stg_airports")
        .withColumn("processed_timestamp", current_timestamp())
        .withColumn("airport_name", trim(col("airport_name")))
        .withColumn("city", trim(col("city")))
        .withColumn("country", trim(col("country")))
        .drop("_rescued_data")
        .dropDuplicates()
    )


dlt.create_streaming_table("silver_airports")
dlt.create_auto_cdc_flow(
    target = "silver_airports",
    source = "trans_airports",
    keys = ["airport_id"],
    sequence_by = col("processed_timestamp"),
    stored_as_scd_type = 1
)