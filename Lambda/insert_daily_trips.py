import json
import boto3
import psycopg2
import random
import os
from datetime import datetime, timedelta


def lambda_handler(event, context):

    # RDS Connection — credentials stored as Lambda Environment Variables
    # Set these in Lambda → Configuration → Environment Variables
    conn = psycopg2.connect(
        host=os.environ['RDS_HOST'],
        port=5432,
        dbname=os.environ['RDS_DBNAME'],
        user=os.environ['RDS_USER'],
        password=os.environ['RDS_PASSWORD'],
        sslmode="require"
    )
    cursor = conn.cursor()

    # NYC taxi zone IDs (real location IDs)
    locations = [132, 161, 236, 237, 186, 170, 48, 79, 68, 234, 141, 262]
    payment_types = [1, 1, 1, 2]  # 1=credit card (more common), 2=cash

    today = datetime.now()
    trips_inserted = 0

    for _ in range(1000):
        # Random hour weighted toward rush hours
        hour = random.choices(
            range(24),
            weights=[1, 1, 1, 1, 1, 2, 3, 4, 5, 4, 3, 3, 4, 4, 3, 3, 5, 6, 6, 5, 4, 3, 2, 1]
        )[0]

        minute = random.randint(0, 59)
        pickup_dt = today.replace(hour=hour, minute=minute, second=0)

        # Trip duration between 5 and 60 minutes
        duration = random.randint(5, 60)
        dropoff_dt = pickup_dt + timedelta(minutes=duration)

        # Trip distance based on duration
        distance = round(random.uniform(0.5, 15.0), 2)

        # Fare calculation (realistic)
        base_fare = 3.00 + (distance * 2.50)
        fare = round(base_fare + random.uniform(-1, 3), 2)
        tip = round(fare * random.uniform(0.10, 0.25), 2) if random.random() > 0.3 else 0
        total = round(fare + tip + 0.50, 2)  # 0.50 surcharge

        cursor.execute("""
            INSERT INTO taxi_trips (
                pickup_datetime, dropoff_datetime, passenger_count,
                trip_distance, fare_amount, tip_amount, total_amount,
                payment_type, pickup_location_id, dropoff_location_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            pickup_dt,
            dropoff_dt,
            random.randint(1, 4),
            distance,
            fare,
            tip,
            total,
            random.choice(payment_types),
            random.choice(locations),
            random.choice(locations)
        ))
        trips_inserted += 1

    conn.commit()
    cursor.close()
    conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Successfully inserted {trips_inserted} trips',
            'date': today.strftime('%Y-%m-%d')
        })
    }
