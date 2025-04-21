import requests
import time
import os
import json
import mysql.connector
from dotenv import load_dotenv
from tabulate import tabulate

# Load environment variables
load_dotenv()

# Constants
API_KEY = os.getenv("N2YO_API_KEY")
SAT_ID = 49810  # You can change this to any NORAD ID
LAT, LON, ALT = 12.97623, 77.60329, 0  # Observer's location (Bangalore)

# MySQL Config
MYSQL_CONFIG = {
    'host': os.getenv("MYSQL_HOST"),
    'user': os.getenv("MYSQL_USER"),
    'password': os.getenv("MYSQL_PASSWORD"),
    'database': os.getenv("MYSQL_DATABASE")
}

# MySQL insert function
def insert_into_mysql(payload):
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        print("✅ Connected to MySQL")

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
        print(f"❌ MySQL error: {err}")


# Fetch from N2YO and Store in MySQL
def fetch_and_store():
    url = f"https://api.n2yo.com/rest/v1/satellite/positions/{SAT_ID}/{LAT}/{LON}/{ALT}/1&apiKey={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        position = data.get("positions", [{}])[0]

        payload = {
            "sat_id": SAT_ID,
            "satname": data.get("info", {}).get("satname", "Unknown"),
            "latitude": position.get("satlatitude", None),
            "longitude": position.get("satlongitude", None),
            "azimuth": position.get("azimuth", None),
            "elevation": position.get("elevation", None),
            "timestamp": position.get("timestamp", None)
        }

        insert_into_mysql(payload)

        print(tabulate([[
            payload["satname"], payload["sat_id"],
            round(payload["latitude"], 4),
            round(payload["longitude"], 4),
            round(payload["azimuth"], 2),
            round(payload["elevation"], 2),
            payload["timestamp"]
        ]], headers=["Satellite", "ID", "Latitude", "Longitude", "Azimuth", "Elevation", "Timestamp"],
           tablefmt="fancy_grid"))

    except requests.RequestException as e:
        print(f"Failed to fetch data: {e}")


# Fetch data from MySQL
def fetch_data_from_mysql():
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        sql = "SELECT satname, latitude, longitude, timestamp FROM satellite_positions ORDER BY timestamp"
        cursor.execute(sql)
        result = cursor.fetchall()

        conn.close()
        return result
    except mysql.connector.Error as err:
        print(f"MySQL error: {err}")
        return []


# Generate HTML and JS for displaying the satellite path
def generate_html_with_map(data):
    # Start HTML page
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Satellite Path</title>
        <link rel="stylesheet" href="https://unpkg.com/leaflet/dist/leaflet.css" />
        <script src="https://unpkg.com/leaflet/dist/leaflet.js"></script>
    </head>
    <body>
        <h2>Satellite Path</h2>
        <div id="map" style="height: 600px;"></div>
        <script>
            var map = L.map('map').setView([12.97623, 77.60329], 13);  // Default to Bangalore coordinates

            // Set the tile layer for the map
            L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
            }).addTo(map);

            var satellitePath = [];
    """
    
    # Add satellite position data to the JavaScript part
    for record in data:
        latitude = record[1]
        longitude = record[2]
        timestamp = record[3]
        html_content += f"""
            satellitePath.push([ {latitude}, {longitude}, "{timestamp}" ]);
        """

    # Add polyline to show the path
    html_content += """
            // Draw polyline to show the path
            var path = L.polyline(satellitePath, { color: 'blue' }).addTo(map);
            // Adjust map view to fit the path
            map.fitBounds(path.getBounds());
        </script>
    </body>
    </html>
    """

    # Save HTML to file
    with open("satellite_path.html", "w") as file:
        file.write(html_content)
    print("Satellite path HTML generated: satellite_path.html")


# Main function to handle periodic fetching and updating of map
def main():
    print("Starting satellite tracking...\n")
    last_update_time = time.time()  # Time for the last map update

    try:
        while True:
            print(f"\nFetching at {time.strftime('%H:%M:%S')}")
            fetch_and_store()  # This function still stores the data into the DB.
            
            current_time = time.time()
            # Update the map every 10 minutes (600 seconds)
            if current_time - last_update_time >= 600:
                # Fetch data from the database and generate updated HTML map
                data = fetch_data_from_mysql()  # Fetch stored data
                generate_html_with_map(data)  # Create the HTML page with map
                last_update_time = current_time  # Update the time of last update

            time.sleep(1)  # Sleep for 1 second before fetching and updating the data

    except KeyboardInterrupt:
        print("\nTracking stopped.")


# Run the main loop
main()
