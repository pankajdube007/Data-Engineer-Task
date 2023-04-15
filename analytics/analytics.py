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
def calculate_distance(loc1, loc2):
    lat1, lon1 = loc1['lat'], loc1['lon']
    lat2, lon2 = loc2['lat'], loc2['lon']
    distance = great_circle((lat1, lon1), (lat2, lon2)).kilometers
    return distance

def aggregate_data():
    # Get current time and the hour before
    current_time = datetime.now()
    previous_hour = current_time - timedelta(hours=1)

    # Create a dictionary to store the aggregated data
    data = {}

    # Query PostgresSQL for the required data
    query = text(f"""
        SELECT device_id, temperature, location, time
        FROM devices
        WHERE time >= '{previous_hour}' AND time < '{current_time}'
    """)
    with psql_engine.connect() as conn:
        result = conn.execute(query)

        # Iterate over the query result and aggregate the data
        for row in result:
            device_id, temperature, location, time = row
            hour = datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:00:00')

            # Calculate the maximum temperature for the device in the current hour
            if device_id in data:
                data[device_id]['max_temperature'] = max(data[device_id]['max_temperature'], temperature)
            else:
                data[device_id] = {'max_temperature': temperature, 'data_points': 0, 'distance': 0}

            # Increment the data points count for the device in the current hour
            data[device_id]['data_points'] += 1

            # Calculate the distance covered by the device in the current hour
            if 'previous_location' in data[device_id]:
                distance = calculate_distance(data[device_id]['previous_location'], location)
                data[device_id]['distance'] += distance
            data[device_id]['previous_location'] = location

    # Insert the aggregated data into the MySQL database
    with mysql_engine.connect() as conn:
        for device_id, values in data.items():
            max_temperature = values['max_temperature']
            data_points = values['data_points']
            distance = values['distance']

            query = text(f"""
                INSERT INTO aggregated_data (device_id, max_temperature, data_points, distance, hour)
                VALUES ('{device_id}', {max_temperature}, {data_points}, {distance}, '{previous_hour}')
            """)
            conn.execute(query)
while True:
    # Wait until the current time is a
    aggregate_data()
