import requests
import time
import os
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# Load API key from .env
load_dotenv()
API_KEY = os.getenv("N2YO_API_KEY")

# Observer's location: example is Bangalore
LAT, LON, ALT = 12.97623, 77.60329, 0

# List of satellite NORAD IDs
SAT_IDS = [49810, 43566, 43056, 41550, 41174]

# Fetch satellite data
def fetch_satellite_data(sat_id):
    url = f"https://api.n2yo.com/rest/v1/satellite/positions/{sat_id}/{LAT}/{LON}/{ALT}/1&apiKey={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        position = data.get("positions", [{}])[0]
        satname = data.get("satname", "Unknown")
        satlatitude = position.get("satlatitude", "N/A")
        satlongitude = position.get("satlongitude", "N/A")
        timestamp = position.get("timestamp", "N/A")

        print(f"{satname} (ID: {sat_id})")
        print(f"    ➤ Latitude  : {satlatitude}")
        print(f"    ➤ Longitude : {satlongitude}")
        print(f"    ➤ Timestamp : {timestamp}\n")

    except requests.RequestException as e:
        print(f"Failed to fetch data for satellite {sat_id}: {e}")

print("Starting satellite data fetch loop...\n")

try:
    while True:
        print(f"Fetching data at {time.strftime('%H:%M:%S')}:\n" + "-" * 60)
        with ThreadPoolExecutor(max_workers=len(SAT_IDS)) as executor:
            executor.map(fetch_satellite_data, SAT_IDS)
        print("-" * 60 + "\n")
        time.sleep(18)

except KeyboardInterrupt:
    print("\nProgram stopped by user.")
