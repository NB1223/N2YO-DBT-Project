from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit, when
from pyspark.sql.types import StructType, DoubleType, StringType, LongType, IntegerType
from Calculations.coverage_overlap import calculate_overlap
from pyspark.sql import Row
from kafka import KafkaProducer
import json
import math

# Set up Kafka Producer for response
response_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Create Spark Session
spark = SparkSession.builder \
    .appName("SatelliteKafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Satellite data schema
satellite_schema = StructType() \
    .add("sat_id", StringType()) \
    .add("satname", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("azimuth", DoubleType()) \
    .add("elevation", DoubleType()) \
    .add("timestamp", LongType())

# Observer schema with renamed fields to prevent ambiguity
observer_schema = StructType() \
    .add("obs_latitude", DoubleType()) \
    .add("obs_longitude", DoubleType()) \
    .add("altitude", DoubleType()) \
    .add("timestamp", LongType()) \
    .add("choice_id", IntegerType())

# Read from Kafka - satellite
df_raw_sat = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribePattern", "satellite-\\d+") \
    .option("startingOffsets", "latest") \
    .load()

# Read from Kafka - observer
df_raw_obs = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "observer_location") \
    .option("startingOffsets", "latest") \
    .load()

# Parse satellite data
df_sat = df_raw_sat.selectExpr("CAST(value AS STRING) as json_string", "topic") \
    .select(from_json(col("json_string"), satellite_schema).alias("data")) \
    .select("data.*")

# Parse observer data and rename lat/lon
df_obs = df_raw_obs.selectExpr("CAST(value AS STRING) as json_string", "topic") \
    .select(from_json(col("json_string"), observer_schema).alias("data")) \
    .select("data.*")

# Cache recent satellite data
# We will use a stateful transformation to store the most recent satellite data
satellite_cache = {}

def update_satellite_cache(new_data):
    sat_id = new_data['sat_id']
    satellite_cache[sat_id] = new_data

def get_cached_satellites():
    return [Row(**satellite_cache[sat_id]) for sat_id in satellite_cache]

# Define action based on choice_id
df_with_choice = df_obs.withColumn(
    "action",
    when(col("choice_id") == 1, lit("motion_vector"))
    .when(col("choice_id") == 2, lit("overlap"))
    .when(col("choice_id") == 3, lit("closest"))
    .when(col("choice_id") == 4, lit("exit"))
    .otherwise(lit("unknown"))
)

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0  # Earth radius in kilometers
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2.0)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2.0)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# Process the satellite data and update cache
def process_satellite_data(batch_df, batch_id):
    batch_df.collect()  # Collect the satellite data into a batch (or any storage mechanism you prefer)
    
    # Update the cache with new satellite data
    for row in batch_df.collect():
        update_satellite_cache(row.asDict())

# Handle the observer choice and calculate coverage overlap
def process_observer_data(df, df_choice):
    if df.filter(col("action") == "motion_vector").isEmpty() != 1:
        pass

    elif df.filter(col("action") == "overlap").isEmpty() != 1:
        cached_satellites = cached_satellites.collect()

        if len(cached_satellites) == 5:
            df_cached_sat = spark.createDataFrame(cached_satellites)

            # called from folder
            overlap_result = calculate_overlap(df_cached_sat)

            print("\n=== Coverage Overlap Result ===")
            overlap_result.show(truncate=False)
            # for row in overlap_result.collect():
            #     response_payload = {
            #         "timestamp": row["timestamp"],
            #         "satname1": row["satname1"],
            #         "satname2": row["satname2"],
            #         "distance_km": round(row["distance_km"], 2),
            #         "overlap": row["overlap"],
            #         "overlap_km": round(row["overlap_km"], 2),
            #         "lat1": row["lat1"],
            #         "lon1": row["lon1"],
            #         "lat2": row["lat2"],
            #         "lon2": row["lon2"]
            #     }
            response_payload_list = []

            for row in overlap_result.collect():
                response_payload_list.append({
                    "timestamp": row["timestamp"],
                    "satname1": row["satname1"],
                    "satname2": row["satname2"],
                    "distance_km": round(row["distance_km"], 2),
                    "overlap": row["overlap"],
                    "overlap_km": round(row["overlap_km"], 2),
                    "lat1": row["lat1"],
                    "lon1": row["lon1"],
                    "lat2": row["lat2"],
                    "lon2": row["lon2"]
                })

            response_producer.send("observer_response", value=response_payload_list)


            # response_producer.send("observer_response", value=response_payload)

        else:
            print("\n[WARN] Not enough satellites in cache to compute overlap.")

    elif df.filter(col("action") == "closest").isEmpty() != 1:
        # Get the latest observer data
        observer_rows = df.collect()
        
        for observer_row in observer_rows:
            action = observer_row["action"]
            
            if action == "closest":
                # Get observer's location
                obs_lat = observer_row["obs_latitude"]
                obs_lon = observer_row["obs_longitude"]
                
                # Find closest satellite
                min_dist = float('inf')
                closest_sat = None
                
                # Convert satellite cache to list of rows
                cached_satellites = [Row(**sat_data) for sat_data in satellite_cache.values()]
                
                if not cached_satellites:
                    print("\n[WARN] No satellites in cache to find closest.")
                    continue
                    
                for sat in cached_satellites:
                    dist = haversine(obs_lat, obs_lon, sat["latitude"], sat["longitude"])
                    if dist < min_dist:
                        min_dist = dist
                        closest_sat = sat
                
                if closest_sat:
                    response = {
                        "satname": closest_sat['satname'],
                        "sat_id": closest_sat['sat_id'],
                        "latitude": closest_sat['latitude'],
                        "longitude": closest_sat['longitude'],
                        "distance_km": round(min_dist, 2),
                        "observer_lat": obs_lat,
                        "observer_lon": obs_lon,
                        "timestamp": observer_row["timestamp"]
                    }
                    print(f"\nClosest satellite found: {response}")
                    response_producer.send("observer_response", value=response)

    else:
        print("exit to be implemented")



# Process the streams
def process_stream():
    # Use an additional foreachBatch to update the satellite cache in each micro-batch
    df_sat.writeStream \
        .foreachBatch(process_satellite_data) \
        .start()

    # Use another foreachBatch to process the observer's choice and calculate overlap when needed
    query = df_with_choice.writeStream \
        .foreachBatch(lambda df, batch_id: process_observer_data(df, df_with_choice)) \
        .start()

    query.awaitTermination()

# Start processing
process_stream()
