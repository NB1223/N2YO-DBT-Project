import requests
import time
import os
import json
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from kafka import KafkaProducer
from tabulate import tabulate
import mysql.connector

load_dotenv()

API_KEY = os.getenv("N2YO_API_KEY")
LAT, LON, ALT = 12.97623, 77.60329, 0
KAFKA_BROKER = 'localhost:9092'
SAT_IDS = [49810, 43566, 43056, 41550, 41174]

MYSQL_CONFIG = {
    'host': os.getenv("MYSQL_HOST"),
    'user': os.getenv("MYSQL_USER"),
    'password': os.getenv("MYSQL_PASSWORD"),
    'database': os.getenv("MYSQL_DATABASE"),
    'auth_plugin': 'mysql_native_password'
}

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

table_data = []

def insert_into_mysql(payload):
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        sql = """
        INSERT INTO satellite_positions 
        (sat_id, satname, latitude, longitude, azimuth, elevation, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            payload["sat_id"],
            payload["satname"],
            payload["latitude"],
            payload["longitude"],
            payload["azimuth"],
            payload["elevation"],
            payload["timestamp"]
        )

        cursor.execute(sql, values)
        conn.commit()
        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"MySQL error: {err}")

def fetch_satellite_data(sat_id):
    url = f"https://api.n2yo.com/rest/v1/satellite/positions/{sat_id}/{LAT}/{LON}/{ALT}/1&apiKey={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        position = data.get("positions", [{}])[0]

        payload = {
            "sat_id": sat_id,
            "satname": data.get("info", {}).get("satname", "Unknown"),
            "latitude": position.get("satlatitude", None),
            "longitude": position.get("satlongitude", None),
            "azimuth": position.get("azimuth", None),
            "elevation": position.get("elevation", None),
            "timestamp": position.get("timestamp", None)
        }

        
        topic = f"satellite-{sat_id}"
        producer.send(topic, value=payload)

        
        if sat_id == 49810:
            insert_into_mysql(payload)

        table_data.append([
            payload["satname"], payload["sat_id"],
            round(payload["latitude"], 4),
            round(payload["longitude"], 4),
            round(payload["azimuth"], 2),
            round(payload["elevation"], 2),
            payload["timestamp"]
        ])

    except requests.RequestException as e:
        print(f"Failed to fetch/send data for satellite {sat_id}: {e}")

print("Starting satellite data producer...\n")

try:
    while True:
        table_data.clear()
        print(f"\nFetching data at {time.strftime('%H:%M:%S')}:\n" + "-" * 80)

        with ThreadPoolExecutor(max_workers=len(SAT_IDS)) as executor:
            executor.map(fetch_satellite_data, SAT_IDS)

        print(tabulate(
            table_data,
            headers=["Satellite", "ID", "Latitude", "Longitude", "Azimuth", "Elevation", "Timestamp"],
            tablefmt="fancy_grid"
        ))

        print("-" * 80)
        time.sleep(18)

except KeyboardInterrupt:
    print("\nProducer stopped by user.")
