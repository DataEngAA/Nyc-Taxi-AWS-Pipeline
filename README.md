# Nyc-Taxi-AWS-Pipeline
End-to-end AWS data pipeline using S3, Glue, Athena, Lambda and EventBridge — built on NYC Taxi data

# Description
An end-to-end data pipeline built on AWS that ingests, transforms and serves NYC Yellow Taxi trip data for analytics. Raw parquet files land in S3, get cleaned and aggregated via AWS Glue using PySpark, auto-cataloged by a Glue Crawler, and made queryable through Athena. The entire pipeline runs automatically every day via a Lambda function triggered by EventBridge scheduler.

# Architecture
Raw Parquet Files → S3 Raw Zone → AWS Glue ETL Job → S3 Processed Zone → Glue Crawler → Glue Data Catalog → Athena → EventBridge + Lambda (automation)

# Tech Stack:
AWS S3, AWS Glue, AWS Athena, AWS Lambda, Amazon EventBridge, PySpark, Python

# Dataset:
NYC Yellow Taxi Trip Records — January 2025 and July 2025, sourced from the NYC Taxi and Limousine Commission.

# Project Structure:
glue/nyc_taxi_etl.py — PySpark ETL script that runs on AWS Glue. Reads raw taxi data, cleans it, adds derived columns and writes daily and hourly aggregations to the processed zone.
lambda/trigger_glue.py — Lambda function that programmatically triggers the Glue ETL job using boto3.
athena/queries.sql — SQL queries used to analyze the processed data including busiest days, January vs July comparison and peak hours analysis.

# How to Run:
Create an S3 bucket with raw, processed and curated folders. Upload NYC Taxi parquet files to the raw zone following the year and month partition structure. Create an IAM role with Glue and S3 permissions. Create a Glue ETL job using the script in the glue folder, attach the IAM role and run it. Create a Glue Crawler pointing to the processed folder to register tables. Open Athena, set a query result location in S3 and run the queries in the athena folder. Optionally set up Lambda and EventBridge to automate the pipeline on a daily schedule.

# Cost:
This project runs almost entirely within AWS free tier. Glue offers 1 million DPU seconds per month free, Athena charges only for data scanned and S3 storage is free up to 5GB. Total cost for this project was under 1 dollar.
