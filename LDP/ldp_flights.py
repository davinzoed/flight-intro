from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp
import dlt


@dp.temporary_view
def stg_flights():

    return (
        spark.readStream
        .format("delta")
        .load("/Volumes/flyproject/bronze/bronze_volume/flights/data/")
    )


@dp.temporary_view
def trans_flights():

    return (
        spark.readStream
        .table("stg_flights")
        .withColumn("flight_date", to_date(col("flight_date")))
        .withColumn("processed_timestamp", current_timestamp())
        .drop("_rescued_data")
        .dropDuplicates()
    )


dlt.create_streaming_table("silver_flights")
dlt.create_auto_cdc_flow(
    target = "silver_flights",
    source = "trans_flights",
    keys = ["flight_id"],
    sequence_by = col("processed_timestamp"),
    stored_as_scd_type = 1
)