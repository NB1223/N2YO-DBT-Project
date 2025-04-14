import requests
import time

# Observer's location: Bangalore
LAT, LON, ALT = 12.97623, 77.60329, 0

# Satellite NORAD IDs for which you want to fetch data; for example, ISS (25544) and another satellite (20580)
SAT_IDS = [25544, 20580]

# Your N2YO API key
API_KEY = "your-api-key"

def fetch_satellite_data(sat_id):
    url = f"https://api.n2yo.com/rest/v1/satellite/positions/{sat_id}/{LAT}/{LON}/{ALT}/1&apiKey={API_KEY}"
    response = requests.get(url)
    if response.ok:
        return response.json()
    else:
        print(f"Error fetching data for sat ID {sat_id}: {response.status_code}")
        return None

print("📡 Starting N2YO satellite stream for Bangalore...\n")

try:
    while True:
        print(f"🔄 Updated @ {time.strftime('%H:%M:%S')}:")
        print("-" * 60)
        
        for sat_id in SAT_IDS:
            data = fetch_satellite_data(sat_id)
            if data is not None:
                # The API returns a "positions" list with one element based on our query '1'
                position = data.get("positions", [{}])[0]
                satname = data.get("satname", "Unknown")
                satlatitude = position.get("satlatitude", "N/A")
                satlongitude = position.get("satlongitude", "N/A")
                azimuth = position.get("azimuth", "N/A")
                elevation = position.get("elevation", "N/A")
                timestamp = position.get("timestamp", "N/A")
                
                print(f"🛰️ {satname} (ID: {sat_id})")
                print(f"     ➤ Latitude   : {satlatitude}")
                print(f"     ➤ Longitude  : {satlongitude}")
                print(f"     ➤ Azimuth    : {azimuth}")
                print(f"     ➤ Elevation  : {elevation}")
                print(f"     ➤ Timestamp  : {timestamp}\n")
        time.sleep(5)

except KeyboardInterrupt:
    print("\n❌ Stopped by user.")
