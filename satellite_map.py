from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, DoubleType, StringType, LongType
import folium
from folium.features import PolyLine
import os
from datetime import datetime

# Create Spark Session
spark = SparkSession.builder \
    .appName("SatelliteKafkaConsumerMap") \
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

# Parse JSON value
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_string", "topic") \
    .select(
        col("topic"),
        from_json(col("json_string"), schema).alias("data")
    ).select("topic", "data.*")

# Store satellite positions for path tracking
satellite_positions = {sat_id: [] for sat_id in ["49810", "43566", "43056", "41550", "41174"]}

# Function to visualize all satellites on one map with lines
def visualize_combined_map(batch_df, epoch_id):
    if batch_df.isEmpty():
        return

    pandas_df = batch_df.toPandas()
    
    # Update positions for each satellite
    for _, row in pandas_df.iterrows():
        sat_id = str(row['sat_id'])
        if sat_id in satellite_positions:
            satellite_positions[sat_id].append((row['latitude'], row['longitude']))
            # Keep only last 50 positions to avoid clutter
            if len(satellite_positions[sat_id]) > 50:
                satellite_positions[sat_id] = satellite_positions[sat_id][-50:]

    # Create a folium map centered at the equator and prime meridian
    fmap = folium.Map(
        location=[0, 0],
        zoom_start=2,
        tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        attr='Esri World Imagery'
    )

    # Color palette for different satellites
    colors = ['red', 'blue', 'green', 'purple', 'orange']
    color_map = {
        "49810": colors[0],
        "43566": colors[1],
        "43056": colors[2],
        "41550": colors[3],
        "41174": colors[4]
    }

    # Add each satellite's path (as a line) and current position
    for sat_id, positions in satellite_positions.items():
        if len(positions) >= 2:
            # Get satellite name from the current batch
            sat_name = pandas_df[pandas_df['sat_id'] == int(sat_id)]['satname'].iloc[0] if not pandas_df[pandas_df['sat_id'] == int(sat_id)].empty else f"Satellite {sat_id}"
            
            # Add line for the path
            PolyLine(
                locations=positions,
                color=color_map.get(sat_id, 'gray'),
                weight=2,
                opacity=0.8,
                popup=f"{sat_name} Path"
            ).add_to(fmap)
            
            # Add current position marker
            if positions:
                folium.Marker(
                    location=positions[-1],
                    popup=f"{sat_name} (ID: {sat_id})",
                    icon=folium.Icon(color=color_map.get(sat_id, 'gray'), icon='satellite', prefix='fa')
                ).add_to(fmap)

    # Add observer location (Bangalore)
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
        sat_name = pandas_df[pandas_df['sat_id'] == int(sat_id)]['satname'].iloc[0] if not pandas_df[pandas_df['sat_id'] == int(sat_id)].empty else f"Sat {sat_id}"
        legend_html += f'<i class="fa fa-circle" style="color:{color}"></i> {sat_name}<br>'
    legend_html += '<i class="fa fa-circle" style="color:black"></i> Observer</div>'
    
    fmap.get_root().html.add_child(folium.Element(legend_html))

    # Add title with timestamp
    title_html = f'''
    <h3 align="center" style="font-size:16px">
    <b>Live Satellite Tracking - {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</b>
    </h3>
    '''
    fmap.get_root().html.add_child(folium.Element(title_html))

    # Save map to HTML
    os.makedirs("maps", exist_ok=True)
    map_path = "maps/combined_satellite_tracking.html"
    fmap.save(map_path)
    
    # Print update message
    print(f"🌍 Combined map updated with data from epoch {epoch_id}")

# Stream with foreachBatch to visualize
query = df_parsed.writeStream \
    .foreachBatch(visualize_combined_map) \
    .outputMode("append") \
    .start()

query.awaitTermination()