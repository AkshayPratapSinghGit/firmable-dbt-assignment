from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    year,
    month,
    to_date,
    datediff,
    avg,
    sum,
    count
)

spark = (
    SparkSession.builder
    .appName("NYC Taxi Historical Processing")
    .getOrCreate()
)

# Read all historical parquet files
df = spark.read.parquet("data/yellow_tripdata_*.parquet")

# Cleaning logic (same as DBT)
clean_df = (
    df
    .filter(col("trip_distance") > 0)
    .filter(col("fare_amount") > 0)
    .filter(col("passenger_count") > 0)
)

daily = (
    clean_df
    .withColumn("trip_date", to_date(col("tpep_pickup_datetime")))
    .groupBy("trip_date")
    .agg(
        count("*").alias("total_trips"),
        sum("fare_amount").alias("total_fare"),
        avg("fare_amount").alias("avg_fare"),
        sum("tip_amount").alias("total_tips")
    )
)

daily = (
    daily
    .withColumn("year", year(col("trip_date")))
    .withColumn("month", month(col("trip_date")))
)
# Repartition by year/month before writing to reduce small files
# and align output partitions with downstream analytical queries.
(
    daily
    .repartition("year", "month")
    .write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet("output/daily_revenue")
)

spark.stop()