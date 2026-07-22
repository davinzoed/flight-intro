from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp

@dp.temporary_view
def stg_bookings():

    df = (
        spark.readStream
        .format("delta")
        .load("/Volumes/flyproject/bronze/bronze_volume/bookings/data/")
    )
    return df


@dp.temporary_view
def trans_bookings():

    df = spark.readStream.table("stg_bookings")

    df = (
        df.withColumn("booking_date", to_date(col("booking_date")))
        .withColumn("amount", col("amount").cast(DoubleType()))
        .withColumn("processed_timestamp", current_timestamp())
        .drop("_rescued_data")
    )
    return df


rules = {
    "rule1": "booking_id IS NOT NULL",
    "rule2": "passenger_id IS NOT NULL"
}

@dp.table
@dp.expect_all_or_drop(rules)
def silver_bookings():
    df = spark.readStream.table("trans_bookings")
    return df

