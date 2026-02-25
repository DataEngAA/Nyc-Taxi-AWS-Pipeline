import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

## Glue boilerplate
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## ---- READ RAW DATA ----
raw_path = "s3://yourname-de-project/raw/yellow_taxi/"

df = spark.read.parquet(raw_path)

print(f"Raw record count: {df.count()}")

## ---- CLEAN DATA ----
df_cleaned = df \
    .dropDuplicates() \
    .filter(F.col("passenger_count") > 0) \
    .filter(F.col("trip_distance") > 0) \
    .filter(F.col("total_amount") > 0) \
    .filter(F.col("tpep_pickup_datetime").isNotNull()) \
    .filter(F.col("tpep_dropoff_datetime").isNotNull())

print(f"Cleaned record count: {df_cleaned.count()}")

## ---- ADD NEW COLUMNS ----
df_enriched = df_cleaned \
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime")) \
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime")) \
    .withColumn("pickup_month", F.month("tpep_pickup_datetime")) \
    .withColumn("trip_duration_mins",
        (F.unix_timestamp("tpep_dropoff_datetime") -
         F.unix_timestamp("tpep_pickup_datetime")) / 60
    ) \
    .filter(F.col("trip_duration_mins") > 0)

## ---- DAILY SUMMARY ----
df_daily = df_enriched.groupBy("pickup_date", "pickup_month").agg(
    F.count("*").alias("total_trips"),
    F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
    F.round(F.avg("total_amount"), 2).alias("avg_fare"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    F.round(F.avg("trip_duration_mins"), 2).alias("avg_duration_mins")
)

## ---- HOURLY SUMMARY ----
df_hourly = df_enriched.groupBy("pickup_date", "pickup_hour").agg(
    F.count("*").alias("total_trips"),
    F.round(F.avg("total_amount"), 2).alias("avg_fare"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue")
)

## ---- WRITE TO PROCESSED ZONE ----
df_daily.write \
    .mode("overwrite") \
    .partitionBy("pickup_month") \
    .parquet("s3://yourname-de-project/processed/yellow_taxi_daily/")

df_hourly.write \
    .mode("overwrite") \
    .partitionBy("pickup_date") \
    .parquet("s3://yourname-de-project/processed/yellow_taxi_hourly/")

print("Job completed successfully!")

job.commit()
