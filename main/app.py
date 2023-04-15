from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.exc import OperationalError
from geopy.distance import great_circle
from datetime import datetime, timedelta
import json

print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        # PostgresSQL database connection
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)

        # MySQL database connection
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')

# Write the solution here



# Get current UTC time rounded to the nearest hour
current_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0)

# Get the start time of the previous hour in UTC
previous_hour = current_time - timedelta(hours=1)

# Define the query to fetch data from the PostgresSQL database
query = text("""
    SELECT device_id, temperature, location, time
    FROM devices
    WHERE time >= :previous_hour AND time < :current_time
""")

# Execute the query and fetch the data
data = psql_engine.execute(query, previous_hour=previous_hour, current_time=current_time)

# Initialize dictionaries to hold aggregated data
max_temperatures = {}
data_points_count = {}
total_distance = {}

# Iterate over the fetched data
for row in data:
    # Extract device ID, temperature, location, and time from the row
    device_id = str(row[0])
    temperature = int(row[1])
    location = json.loads(row[2])
    time = row[3]
    
    # Calculate the distance between the current location and the previous location
    if device_id in total_distance:
        previous_location = total_distance[device_id]["location"]
        current_location = (location["lat"], location["lon"])
        distance = great_circle(previous_location, current_location).kilometers
    else:
        distance = 0
    
    # Update the max temperature, data points count, and total distance for the current device
    if device_id in max_temperatures:
        max_temperatures[device_id] = max(max_temperatures[device_id], temperature)
        data_points_count[device_id] += 1
        total_distance[device_id]["distance"] += distance
        total_distance[device_id]["location"] = current_location
    else:
        max_temperatures[device_id] = temperature
        data_points_count[device_id] = 1
        total_distance[device_id] = {"distance": distance, "location": current_location}
    
# Initialize a list to hold the aggregated data
aggregated_data = []

# Iterate over the devices to construct the aggregated data
for device_id in max_temperatures:
    max_temperature = max_temperatures[device_id]
    data_points = data_points_count[device_id]
    distance = total_distance[device_id]["distance"]
    
    # Construct the aggregated data for the current device
    aggregated_data.append({
        "device_id": device_id,
        "max_temperature": max_temperature,
        "data_points": data_points,
        "distance": distance
    })

# Define the query to insert the aggregated data into the MySQL database
query = text("""
    INSERT INTO aggregated_data (device_id, max_temperature, data_points, distance)
    VALUES (:device_id, :max_temperature, :data_points, :distance)
""")

# Execute the query for each row of aggregated data
for row in aggregated_data:
    mysql_engine.execute(query, **row)
