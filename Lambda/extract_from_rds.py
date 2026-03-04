import json
import boto3
import psycopg2
import pandas as pd
import io
import os
from datetime import datetime, timedelta, timezone

def lambda_handler(event, context):
    
    # IST = UTC + 5:30 (no external library needed)
    IST = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(IST)
    yesterday = now_ist - timedelta(days=1)
    
    date_str = yesterday.strftime('%Y-%m-%d')
    year = yesterday.strftime('%Y')
    month = yesterday.strftime('%m')
    day = yesterday.strftime('%d')
    
    print(f"Extracting trips for date: {date_str} (IST yesterday)")
    
    # Connect to RDS
    conn = psycopg2.connect(
        host=os.environ['DB_HOST'],
        port=5432,
        dbname=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        sslmode="require"
    )
    
    # Extract yesterday's trips
    query = f"""
        SELECT 
            trip_id,
            pickup_datetime,
            dropoff_datetime,
            passenger_count,
            trip_distance,
            fare_amount,
            tip_amount,
            total_amount,
            payment_type,
            pickup_location_id,
            dropoff_location_id,
            created_at
        FROM taxi_trips
        WHERE DATE(pickup_datetime) = '{date_str}'
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f"Extracted {len(df)} trips for {date_str}")
    
    if len(df) == 0:
        return {
            'statusCode': 200,
            'body': json.dumps({'message': f'No trips found for {date_str}'})
        }
    
    # Convert to Parquet in memory
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, coerce_timestamps='us')
    buffer.seek(0)
    
    # Upload to S3
    s3 = boto3.client('s3')
    s3_key = f"raw/rds_trips/year={year}/month={month}/day={day}/trips_{date_str}.parquet"
    
    s3.put_object(
        Bucket=os.environ['S3_BUCKET'],
        Key=s3_key,
        Body=buffer.getvalue()
    )
    
    print(f"Uploaded to s3://{os.environ['S3_BUCKET']}/{s3_key}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Successfully extracted and uploaded {len(df)} trips',
            'date': date_str,
            's3_path': f"s3://{os.environ['S3_BUCKET']}/{s3_key}"
        })
    }
