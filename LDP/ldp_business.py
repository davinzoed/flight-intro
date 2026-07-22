from pyspark import pipelines as dp

@dp.table
def silver_business():

    return (
        spark.readStream.table("silver_bookings")
        .join(
            spark.readStream.table("silver_flights"),
            "flight_id"
        )
        .join(
            spark.readStream.table("silver_passengers"),
            "passenger_id"
        )
        .join(
            spark.readStream.table("silver_airports"),
            "airport_id"
        )
        .drop("processed_timestamp")
    )