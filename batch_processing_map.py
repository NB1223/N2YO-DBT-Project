import os
import time
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

MYSQL_CONFIG = {
    'host': os.getenv("MYSQL_HOST"),
    'user': os.getenv("MYSQL_USER"),
    'password': os.getenv("MYSQL_PASSWORD"),
    'database': os.getenv("MYSQL_DATABASE"),
    'auth_plugin': 'mysql_native_password'
}

def fetch_data_from_mysql():
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        print("Connected to MySQL")
        cursor = conn.cursor()

        sql = """
            SELECT satname, latitude, longitude, timestamp
            FROM satellite_positions
            WHERE sat_id = 49810
            ORDER BY timestamp
        """
        cursor.execute(sql)
        result = cursor.fetchall()

        cursor.close()
        conn.close()
        return result
    except mysql.connector.Error as err:
        print(f"MySQL error: {err}")
        return []

def generate_html_with_map(data):
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Satellite 49810 Path</title>
        <link rel="stylesheet" href="https://unpkg.com/leaflet/dist/leaflet.css" />
        <script src="https://unpkg.com/leaflet/dist/leaflet.js"></script>
    </head>
    <body>
        <h2>Satellite 49810 Path</h2>
        <div id="map" style="height: 600px;"></div>
        <script>
            var map = L.map('map').setView([12.97623, 77.60329], 4);

            L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                attribution: '&copy; OpenStreetMap contributors'
            }).addTo(map);

            var satellitePath = [];
    """

    for record in data:
        latitude = record[1]
        longitude = record[2]
        timestamp = record[3]
        html_content += f"""
            satellitePath.push([{latitude}, {longitude}]);
            L.circleMarker([{latitude}, {longitude}], {{
                radius: 4,
                color: 'blue',
                fillOpacity: 0.6
            }}).addTo(map).bindPopup("Timestamp: {timestamp}");
        """

    html_content += """
            var path = L.polyline(satellitePath, { color: 'red' }).addTo(map);
            map.fitBounds(path.getBounds());
        </script>
    </body>
    </html>
    """

    with open("satellite_path.html", "w") as file:
        file.write(html_content)

    print("Satellite map updated: satellite_path.html")

def main():
    print("📡 Watching for new satellite data... (updates every 10 minutes)\n")
    last_update_time = 0

    try:
        while True:
            current_time = time.time()
            if current_time - last_update_time >= 600:
                data = fetch_data_from_mysql()
                if data:
                    generate_html_with_map(data)
                    last_update_time = current_time
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nExiting map.")

if __name__ == "__main__":
    main()
