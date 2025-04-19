from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, LongType
import math
from kafka import KafkaProducer
import json

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
MOTION_VECTOR_TOPIC = "motion_vector_response"

# Kafka Producer for sending motion vector results
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Create Spark Session
spark = SparkSession.builder \
    .appName("MotionVectorConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema for satellite messages
satellite_schema = StructType() \
    .add("sat_id", StringType()) \
    .add("satname", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("azimuth", DoubleType()) \
    .add("elevation", DoubleType()) \
    .add("timestamp", LongType())

# Read satellite data from Kafka
satellite_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribePattern", "satellite-\\d+") \
    .option("startingOffsets", "latest") \
    .load()

satellite_parsed = satellite_df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), satellite_schema).alias("data")) \
    .select("data.*")

# Cache last position of each satellite
last_satellite_position = {}

# Haversine distance calculation
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2.0)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2.0)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def calculate_motion_vector(prev, curr):
    lat1, lon1, time1 = prev['latitude'], prev['longitude'], prev['timestamp']
    lat2, lon2, time2 = curr['latitude'], curr['longitude'], curr['timestamp']

    distance = haversine(lat1, lon1, lat2, lon2)
    time_diff = time2 - time1  # seconds

    if time_diff == 0:
        return 0, (0, 0)

    velocity = distance / (time_diff / 3600)  # km/h
    direction = (lat2 - lat1, lon2 - lon1)
    return velocity, direction

# Process satellite stream to compute motion vector
def process_motion_vector(df, epoch_id):
    rows = df.collect()
    for row in rows:
        sat_id = row['sat_id']

        if sat_id in last_satellite_position:
            prev = last_satellite_position[sat_id]
            curr = {
                'latitude': row['latitude'],
                'longitude': row['longitude'],
                'timestamp': row['timestamp']
            }

            velocity, direction = calculate_motion_vector(prev, curr)

            response = {
                "sat_id": sat_id,
                "satname": row['satname'],
                "velocity_kmph": round(velocity, 2),
                "direction": {
                    "delta_latitude": round(direction[0], 5),
                    "delta_longitude": round(direction[1], 5)
                },
                "timestamp": row['timestamp']
            }

            print(f"[MOTION VECTOR] {response}")
            producer.send(MOTION_VECTOR_TOPIC, value=response)

        # Update last position
        last_satellite_position[sat_id] = {
            'latitude': row['latitude'],
            'longitude': row['longitude'],
            'timestamp': row['timestamp']
        }

# Start the stream
satellite_parsed.writeStream \
    .foreachBatch(process_motion_vector) \
    .start() \
    .awaitTermination()

