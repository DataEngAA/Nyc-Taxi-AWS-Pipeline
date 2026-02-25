-- Top 5 busiest days by trip count
SELECT pickup_date, total_trips, total_revenue
FROM nyc_taxi_db.yellow_taxi_daily
ORDER BY total_trips DESC
LIMIT 5;

-- January vs July comparison
SELECT pickup_month,
    SUM(total_trips) as total_trips,
    ROUND(SUM(total_revenue), 2) as total_revenue,
    ROUND(AVG(avg_fare), 2) as avg_fare
FROM nyc_taxi_db.yellow_taxi_daily
GROUP BY pickup_month
ORDER BY pickup_month;

-- Top 5 busiest hours of the day
SELECT pickup_hour,
    SUM(total_trips) as total_trips,
    ROUND(AVG(avg_fare), 2) as avg_fare
FROM nyc_taxi_db.yellow_taxi_hourly
GROUP BY pickup_hour
ORDER BY total_trips DESC
LIMIT 5;
