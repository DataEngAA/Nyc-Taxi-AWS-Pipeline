# 🚕 NYC Taxi AWS Data Pipeline

End-to-end, fully automated AWS data pipeline using S3, Glue, Athena, Lambda, RDS, EventBridge, CloudWatch and Grafana — built on NYC Taxi data with real-time weather enrichment.

## 📋 Description

An end-to-end AWS data engineering pipeline that ingests, transforms, and visualizes NYC taxi trip data from multiple sources — fully automated with nightly orchestration via AWS Step Functions.

The entire pipeline runs automatically every night via EventBridge scheduling with no manual intervention required. CloudWatch alarms send email alerts if anything fails.

---

## 🏗️ Architecture

```
Data Sources
├── NYC TLC Yellow Taxi Parquet  (static, Jan + Jul 2025, 7.3M trips)
├── RDS PostgreSQL               (daily Lambda-generated trips)
└── OpenWeather API              (daily NYC weather)
         │
         ▼  AWS Lambda (nightly ingestion)
         │
S3 Raw Layer (Bronze)
├── raw/yellow_taxi/
├── raw/rds_trips/year=Y/month=M/day=D/
└── raw/weather/year=Y/month=M/day=D/
         │
         ▼  AWS Glue ETL (PySpark)
         │  Clean → Transform → Aggregate → Join
         │
S3 Processed Layer (Gold)
├── processed/yellow_taxi_daily/
├── processed/yellow_taxi_hourly/
└── processed/rds_trips_with_weather/
         │
         ▼  Glue Crawler → Glue Data Catalog
         │
         ▼  Amazon Athena (Serverless SQL)
         │
         ▼  Grafana Dashboard (EC2 t2.micro)

CloudWatch Alarms + SNS → Email alerts on any failure
EventBridge → Fully automated nightly schedule
```

---

## ⚙️ Tech Stack

| Service | Purpose |
|---|---|
| Amazon S3 | Data lake — raw + processed layers (Bronze/Gold) |
| AWS Glue ETL | PySpark transformation, aggregation and joins |
| AWS Glue Crawler | Auto-catalog schemas and partitions in Data Catalog |
| Amazon Athena | Serverless SQL queries directly on S3 |
| RDS PostgreSQL | Operational database — daily trip data source |
| AWS Lambda | 3 serverless ingestion functions |
| Amazon EventBridge | Cron-based nightly scheduling (5 rules) |
| Amazon CloudWatch | Monitor Lambda + Glue for failures |
| Amazon SNS | Email alerts when pipeline fails |
| Amazon EC2 | Hosts Grafana dashboard server (t2.micro) |
| Grafana | Live visualization dashboard (7 panels) |
| OpenWeather API | Real-time NYC weather data |
| PySpark | Distributed data processing inside Glue |
| Python | Lambda functions and data processing |

## Automated Nightly Schedule

| Time (UTC) | Step | What It Does |
|---|---|---|
| 22:00 | Lambda: insert-daily-trips | Inserts ~1000 realistic trips into RDS PostgreSQL |
| 23:00 | Lambda: extract-from-rds | Extracts yesterday's RDS trips → S3 parquet |
| 23:30 | Lambda: ingest-weather-data | Fetches NYC weather → S3 JSON |
| 01:00 | Glue ETL: nyc-taxi-etl | Transforms all 3 sources, joins RDS + weather, writes to S3 |
| 02:00 | Glue Crawler | Catalogs new S3 partitions in Athena Data Catalog |

Every morning, new data is automatically available in Athena and reflected in Grafana — zero manual steps required.

## 🗄️ Dataset

**NYC Yellow Taxi Trip Records:**
- January 2025 and July 2025
- Sourced from the NYC Taxi and Limousine Commission (TLC)
- 7,374,189 total trip records

**RDS Daily Trips:**
- ~1000 synthetic trips generated nightly via Lambda
- Stored in PostgreSQL, extracted to S3 as parquet daily

**Weather Data:**
- Daily NYC weather from OpenWeather API
- Temperature, humidity, wind speed, conditions



## Key Technical Lessons

**1. Parquet + Spark timestamp compatibility:**
```python
# WRONG — pandas defaults to NANOS, Spark cannot read this
df.to_parquet(buffer, index=False)

# CORRECT — force microseconds for Spark compatibility
df.to_parquet(buffer, index=False, coerce_timestamps='us')
```

**2. Spark reading single JSON objects:**
```python
# WRONG — Spark expects newline-delimited JSON by default
df = spark.read.json(path)

# CORRECT — use multiLine for single JSON objects per file
df = spark.read.option("multiLine", "true").schema(schema).json(path)
```

**3. Always print full errors in Glue:**
```python
# Silent failures are the hardest to debug
except Exception as e:
    print(f"ERROR: {str(e)}")  # always log the full error
```

---

## 🚀 How to Reproduce
1. Create S3 bucket with `raw/` and `processed/` folders
2. Upload NYC Taxi parquet files to `raw/yellow_taxi/`
3. Create RDS PostgreSQL instance and `taxi_trips` table
4. Deploy 3 Lambda functions with required layers (psycopg2, pandas, requests)
5. Create Glue ETL job using script in `glue/` — add `--S3_BUCKET` job parameter
6. Create Glue Crawler pointing to `s3://your-bucket/processed/`
7. Set up 5 EventBridge rules for nightly scheduling
8. Create CloudWatch alarms + SNS topic for failure alerts
9. Launch EC2 t2.micro, install Grafana, connect Athena as data source

## 💰 Cost

This project runs almost entirely within the AWS free tier:
- **Glue** — 1 million DPU-seconds free per month
- **Athena** — pay per query (~$5/TB scanned, queries cost fractions of a cent)
- **Lambda** — 1 million free requests per month
- **S3** — free up to 5GB
- **RDS** — db.t3.micro free tier (750 hours/month)
- **EC2** — t2.micro free tier (750 hours/month)

Total monthly cost for this project: **under $1**

## Monitoring & Alerts
CloudWatch Alarms on each Lambda and Glue step
SNS email notifications on failure
Step Functions execution history for full audit trail
Grafana dashboard for visual monitoring of trip and weather trends


## Key Design Decisions
Step Functions over individual EventBridge rules — replaced 4 independent EventBridge rules with a single orchestrated state machine, adding proper sequencing, dependency control, and failure-safe execution.
Bronze/Gold S3 layers — raw data preserved in Bronze, clean aggregated data in Gold for Athena queries.
Partitioned S3 paths — year=Y/month=M/day=D/ structure enables efficient Athena partition pruning.
Glue Crawler — automatically detects new partitions so Athena always queries fresh data without manual schema updates.
