from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp
import dlt


@dp.temporary_view
def stg_passengers():

    return (
        spark.readStream
        .format("delta")
        .load("/Volumes/flyproject/bronze/bronze_volume/passengers/data/")
    )


@dp.temporary_view
def trans_passengers():

    return (
        spark.readStream
        .table("stg_passengers")
        .withColumn("processed_timestamp", current_timestamp())
        .withColumn("name", trim(col("name")))
        .withColumn("gender", trim(col("gender")))
        .withColumn("nationality", trim(col("nationality")))
        .drop("_rescued_data")
        .dropDuplicates()
    )


dlt.create_streaming_table("silver_passengers")
dlt.create_auto_cdc_flow(
    target = "silver_passengers",
    source = "trans_passengers",
    keys = ["passenger_id"],
    sequence_by = col("processed_timestamp"),
    stored_as_scd_type = 1
)