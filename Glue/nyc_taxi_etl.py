import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# ---------------------------------------------------------------
# NYC Taxi AWS Data Pipeline — Glue ETL Script
# ---------------------------------------------------------------
# Sources:
#   1. Yellow Taxi parquet  (static, s3://BUCKET/raw/yellow_taxi/)
#   2. RDS trips parquet    (daily, s3://BUCKET/raw/rds_trips/)
#   3. Weather JSON         (daily, s3://BUCKET/raw/weather/)
#
# Outputs:
#   1. yellow_taxi_daily        (partitioned by pickup_month)
#   2. yellow_taxi_hourly       (partitioned by pickup_date)
#   3. rds_trips_with_weather   (partitioned by pickup_date)
#
# Key lessons:
#   - RDS parquet must be written with coerce_timestamps='us' in Lambda
#     (pandas defaults to NANOS which Spark cannot read)
#   - Weather JSON must be read with multiLine=true
#     (single JSON objects per file, not newline-delimited)
# ---------------------------------------------------------------

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 bucket passed as Glue job parameter
# Set in Glue → Job details → Job parameters → Key: --S3_BUCKET Value: your-bucket-name
BUCKET = args['S3_BUCKET']

## ---- SOURCE 1: YELLOW TAXI ----
print("=== Reading yellow taxi data ===")
df_taxi = spark.read.parquet(f"s3://{BUCKET}/raw/yellow_taxi/")
print(f"Taxi records: {df_taxi.count()}")

df_taxi_cleaned = df_taxi \
    .dropDuplicates() \
    .filter(F.col("passenger_count") > 0) \
    .filter(F.col("trip_distance") > 0) \
    .filter(F.col("total_amount") > 0) \
    .filter(F.col("tpep_pickup_datetime").isNotNull())

df_taxi_enriched = df_taxi_cleaned \
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime")) \
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime")) \
    .withColumn("pickup_month", F.month("tpep_pickup_datetime")) \
    .withColumn("trip_duration_mins",
        (F.unix_timestamp("tpep_dropoff_datetime") -
         F.unix_timestamp("tpep_pickup_datetime")) / 60) \
    .filter(F.col("trip_duration_mins") > 0)

df_daily = df_taxi_enriched.groupBy("pickup_date", "pickup_month").agg(
    F.count("*").alias("total_trips"),
    F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
    F.round(F.avg("total_amount"), 2).alias("avg_fare"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    F.round(F.avg("trip_duration_mins"), 2).alias("avg_duration_mins")
)

df_hourly = df_taxi_enriched.groupBy("pickup_date", "pickup_hour").agg(
    F.count("*").alias("total_trips"),
    F.round(F.avg("total_amount"), 2).alias("avg_fare"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue")
)

## ---- SOURCE 2: RDS TRIPS ----
print("=== Reading RDS trips ===")
try:
    df_rds = spark.read.parquet(f"s3://{BUCKET}/raw/rds_trips/")
    print(f"RDS raw records: {df_rds.count()}")
    df_rds.printSchema()
    df_rds.show(3)

    df_rds_clean = df_rds \
        .filter(F.col("passenger_count") > 0) \
        .filter(F.col("trip_distance") > 0) \
        .filter(F.col("total_amount") > 0) \
        .withColumn("pickup_date", F.to_date("pickup_datetime")) \
        .withColumn("pickup_hour", F.hour("pickup_datetime")) \
        .withColumn("trip_duration_mins",
            (F.unix_timestamp("dropoff_datetime") -
             F.unix_timestamp("pickup_datetime")) / 60) \
        .filter(F.col("trip_duration_mins") > 0)

    df_rds_daily = df_rds_clean.groupBy("pickup_date").agg(
        F.count("*").alias("total_trips"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("trip_duration_mins"), 2).alias("avg_duration_mins")
    )
    print(f"RDS daily rows: {df_rds_daily.count()}")
    df_rds_daily.show()

except Exception as e:
    print(f"ERROR reading RDS trips: {str(e)}")
    df_rds_daily = None

## ---- SOURCE 3: WEATHER ----
print("=== Reading weather data ===")
try:
    # Explicit schema required — prevents type inference errors
    # multiLine=true required — weather files are single JSON objects
    weather_schema = StructType([
        StructField("date", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("city", StringType(), True),
        StructField("temperature_f", DoubleType(), True),
        StructField("feels_like_f", DoubleType(), True),
        StructField("humidity_pct", IntegerType(), True),
        StructField("weather_condition", StringType(), True),
        StructField("weather_description", StringType(), True),
        StructField("wind_speed_mph", DoubleType(), True),
        StructField("visibility_meters", IntegerType(), True),
        StructField("rain_1h_mm", DoubleType(), True),
        StructField("snow_1h_mm", DoubleType(), True)
    ])

    df_weather = spark.read \
        .option("multiLine", "true") \
        .schema(weather_schema) \
        .json(f"s3://{BUCKET}/raw/weather/")

    print(f"Weather records: {df_weather.count()}")
    df_weather.printSchema()
    df_weather.show()

    df_weather_clean = df_weather \
        .filter(F.col("date").isNotNull()) \
        .select(
            F.to_date("date").alias("pickup_date"),
            "temperature_f",
            "feels_like_f",
            "humidity_pct",
            "weather_condition",
            "weather_description",
            "wind_speed_mph",
            "rain_1h_mm",
            "snow_1h_mm"
        )
    print(f"Weather clean records: {df_weather_clean.count()}")
    df_weather_clean.show()

except Exception as e:
    print(f"ERROR reading weather: {str(e)}")
    df_weather_clean = None

## ---- JOIN RDS TRIPS + WEATHER ----
print("=== Joining ===")
if df_rds_daily is not None and df_weather_clean is not None:
    print("Both dataframes available — joining...")
    df_trips_weather = df_rds_daily.join(df_weather_clean, on="pickup_date", how="left")
    print(f"Joined rows: {df_trips_weather.count()}")
    df_trips_weather.show()

    df_trips_weather.write \
        .mode("overwrite") \
        .partitionBy("pickup_date") \
        .parquet(f"s3://{BUCKET}/processed/rds_trips_with_weather/")
    print("Written to S3 successfully!")
else:
    print(f"Skipping join — rds_daily: {df_rds_daily is not None}, weather: {df_weather_clean is not None}")

## ---- WRITE YELLOW TAXI OUTPUTS ----
print("=== Writing yellow taxi outputs ===")
df_daily.write \
    .mode("overwrite") \
    .partitionBy("pickup_month") \
    .parquet(f"s3://{BUCKET}/processed/yellow_taxi_daily/")

df_hourly.write \
    .mode("overwrite") \
    .partitionBy("pickup_date") \
    .parquet(f"s3://{BUCKET}/processed/yellow_taxi_hourly/")

print("=== All done! ===")
job.commit()
