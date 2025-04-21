from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, current_timestamp
from pyspark.sql.types import StructType, DoubleType, StringType, LongType
import folium
from folium.features import PolyLine
import os
from datetime import datetime

# Create Spark Session
spark = SparkSession.builder \
    .appName("SatelliteKafkaConsumerMapWithWindows") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema for satellite data
schema = StructType() \
    .add("sat_id", StringType()) \
    .add("satname", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("azimuth", DoubleType()) \
    .add("elevation", DoubleType()) \
    .add("timestamp", LongType())

# Kafka readStream
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribePattern", "satellite-\\d+") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON value and add processing timestamp
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_string", "topic") \
    .select(
        col("topic"),
        from_json(col("json_string"), schema).alias("data")
    ).select("topic", "data.*") \
    .withColumn("processing_time", current_timestamp())

# Apply sliding window: 30-minute window sliding every 2 minutes
windowed_df = df_parsed \
    .withWatermark("processing_time", "10 minutes") \
    .groupBy(
        window("processing_time", "30 minutes", "2 minutes"),
        col("sat_id"),
        col("satname")
    ) \
    .agg(
        {"latitude": "last", "longitude": "last", "timestamp": "max"}
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("sat_id"),
        col("satname"),
        col("last(latitude)").alias("latitude"),
        col("last(longitude)").alias("longitude"),
        col("max(timestamp)").alias("timestamp")
    )

# Storage for windowed data
windowed_positions = {sat_id: [] for sat_id in ["49810", "43566", "43056", "41550", "41174"]}

# Function to visualize with sliding windows
def visualize_windowed_map(batch_df, epoch_id):
    if batch_df.isEmpty():
        return

    pandas_df = batch_df.toPandas()
    
    # Update positions for each satellite
    for _, row in pandas_df.iterrows():
        sat_id = str(row['sat_id'])
        if sat_id in windowed_positions:
            windowed_positions[sat_id].append({
                "position": (row['latitude'], row['longitude']),
                "window_start": row['window_start'],
                "window_end": row['window_end']
            })
            # Keep only positions from the last 30-minute window
            current_time = datetime.now()
            windowed_positions[sat_id] = [
                pos for pos in windowed_positions[sat_id] 
                if (current_time - pos['window_end']).total_seconds() <= 1800  # 30 minutes
            ]

    # Create map
    fmap = folium.Map(
        location=[0, 0],
        zoom_start=2,
        tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        attr='Esri World Imagery'
    )

    # Color palette
    colors = ['red', 'blue', 'green', 'purple', 'orange']
    color_map = {
        "49810": colors[0],
        "43566": colors[1],
        "43056": colors[2],
        "41550": colors[3],
        "41174": colors[4]
    }

    # Add paths and markers
    for sat_id, positions in windowed_positions.items():
        if len(positions) >= 2:
            # Extract just the coordinates for the path
            coords = [pos['position'] for pos in positions]
            
            # Add path line
            PolyLine(
                locations=coords,
                color=color_map.get(sat_id, 'gray'),
                weight=2,
                opacity=0.8,
                popup=f"{positions[0]['satname'] if 'satname' in positions[0] else 'Satellite'} Path"
            ).add_to(fmap)
            
            # Add current position marker
            folium.Marker(
                location=coords[-1],
                popup=f"{positions[-1].get('satname', 'Satellite')} (ID: {sat_id})<br>"
                      f"Window: {positions[-1]['window_start']} to {positions[-1]['window_end']}",
                icon=folium.Icon(color=color_map.get(sat_id, 'gray'), icon='satellite', prefix='fa')
            ).add_to(fmap)

    # Add observer location
    folium.Marker(
        location=[12.97623, 77.60329],
        popup="Observer: Bangalore",
        icon=folium.Icon(color='black', icon='user')
    ).add_to(fmap)

    # Add legend
    legend_html = '''
    <div style="position: fixed; 
                bottom: 50px; left: 50px; width: 180px; height: 150px; 
                border:2px solid grey; z-index:9999; font-size:14px;
                background-color:white;
                opacity: 0.8;">
    <b>Satellite Legend</b><br>
    '''
    for sat_id, color in color_map.items():
        sat_name = next((p.get('satname', f"Sat {sat_id}") for p in windowed_positions[sat_id]), f"Sat {sat_id}")
        legend_html += f'<i class="fa fa-circle" style="color:{color}"></i> {sat_name}<br>'
    legend_html += '<i class="fa fa-circle" style="color:black"></i> Observer</div>'
    
    fmap.get_root().html.add_child(folium.Element(legend_html))

    # Add title with time range
    if windowed_positions:
        latest_window = max(pos['window_end'] for positions in windowed_positions.values() for pos in positions)
        oldest_window = min(pos['window_start'] for positions in windowed_positions.values() for pos in positions)
        title_html = f'''
        <h3 align="center" style="font-size:16px">
        <b>Satellite Tracking - {oldest_window.strftime("%H:%M")} to {latest_window.strftime("%H:%M")}</b>
        </h3>
        '''
        fmap.get_root().html.add_child(folium.Element(title_html))

    # Save map
    os.makedirs("maps", exist_ok=True)
    map_path = "maps/windowed_satellite_tracking.html"
    fmap.save(map_path)
    
    print(f"🌍 Map updated with 30-minute sliding window data (epoch {epoch_id})")

# Start the streaming query
query = windowed_df.writeStream \
    .foreachBatch(visualize_windowed_map) \
    .outputMode("complete") \
    .start()

query.awaitTermination()