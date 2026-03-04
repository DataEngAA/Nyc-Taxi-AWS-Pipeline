import json
import boto3
import urllib.request
import urllib.parse
import os
from datetime import datetime, timedelta, timezone

def lambda_handler(event, context):
    
    # OpenWeather API config
    api_key = os.environ['OPENWEATHER_API_KEY']
    city = "New York"
    encoded_city = urllib.parse.quote(city)
    url = f"https://api.openweathermap.org/data/2.5/weather?q={encoded_city}&appid={api_key}&units=imperial"
    
    # Fetch weather data
    with urllib.request.urlopen(url) as response:
        weather_raw = json.loads(response.read().decode())
    
    # IST = UTC + 5:30 (no external library needed)
    IST = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(IST)
    yesterday = now_ist - timedelta(days=1)
    
    weather_data = {
        "date": yesterday.strftime('%Y-%m-%d'),
        "timestamp": now_ist.strftime('%Y-%m-%d %H:%M:%S'),
        "city": city,
        "temperature_f": weather_raw["main"]["temp"],
        "feels_like_f": weather_raw["main"]["feels_like"],
        "humidity_pct": weather_raw["main"]["humidity"],
        "weather_condition": weather_raw["weather"][0]["main"],
        "weather_description": weather_raw["weather"][0]["description"],
        "wind_speed_mph": weather_raw["wind"]["speed"],
        "visibility_meters": weather_raw.get("visibility", 0),
        "rain_1h_mm": weather_raw.get("rain", {}).get("1h", 0),
        "snow_1h_mm": weather_raw.get("snow", {}).get("1h", 0)
    }
    
    print(f"Weather fetched: {weather_data['weather_condition']}, {weather_data['temperature_f']}F")
    
    # Upload to S3 using yesterday's date (IST)
    s3 = boto3.client('s3')
    year = yesterday.strftime('%Y')
    month = yesterday.strftime('%m')
    day = yesterday.strftime('%d')
    
    s3_key = f"raw/weather/year={year}/month={month}/day={day}/weather_{yesterday.strftime('%Y-%m-%d')}.json"
    
    s3.put_object(
        Bucket=os.environ['S3_BUCKET'],
        Key=s3_key,
        Body=json.dumps(weather_data, indent=2),
        ContentType="application/json"
    )
    
    print(f"Uploaded to s3://{os.environ['S3_BUCKET']}/{s3_key}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Weather data ingested successfully',
            'date': weather_data['date'],
            'condition': weather_data['weather_condition'],
            'temperature_f': weather_data['temperature_f'],
            's3_path': f"s3://{os.environ['S3_BUCKET']}/{s3_key}"
        })
    }
