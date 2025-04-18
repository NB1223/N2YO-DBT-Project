from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, DoubleType, StringType, LongType
import math
from kafka import KafkaProducer
import json

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
OBSERVER_TOPIC = "observer_location"
RESPONSE_TOPIC = "observer_response"

# Kafka Producer for sending responses
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Create Spark Session
spark = SparkSession.builder \
    .appName("SatelliteClosestObserver") \
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

# Schema for observer requests
observer_schema = StructType() \
    .add("obs_latitude", DoubleType()) \
    .add("obs_longitude", DoubleType()) \
    .add("altitude", DoubleType()) \
    .add("timestamp", LongType()) \
    .add("choice_id", LongType())

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

# Cache latest satellite data in memory
from pyspark.sql.streaming import DataStreamWriter

latest_satellite_data = {}

def update_latest_satellite_data(row):
    latest_satellite_data[row['sat_id']] = row

def start_satellite_collector():
    def foreach_batch_function(df, epoch_id):
        rows = df.collect()
        for row in rows:
            update_latest_satellite_data(row)

    satellite_parsed.writeStream \
        .foreachBatch(foreach_batch_function) \
        .start()

# Read observer requests
observer_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", OBSERVER_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

observer_parsed = observer_df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), observer_schema).alias("data")) \
    .select("data.*")

# Haversine distance calculation
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0  # Earth radius in kilometers
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2.0)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2.0)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def process_observer_requests(df, epoch_id):
    requests = df.collect()
    for req in requests:
        if req.choice_id == 3:
            min_dist = float('inf')
            closest_sat = None
            for sat in latest_satellite_data.values():
                dist = haversine(req.obs_latitude, req.obs_longitude, sat['latitude'], sat['longitude'])
                if dist < min_dist:
                    min_dist = dist
                    closest_sat = sat

            if closest_sat:
                # response = {
                #     "choice_id": 3,
                #     "closest_satellite": {
                #         "satname": closest_sat['satname'],
                #         "sat_id": closest_sat['sat_id'],
                #         "latitude": closest_sat['latitude'],
                #         "longitude": closest_sat['longitude'],
                #         "distance_km": round(min_dist, 2)
                #     }
                # }
                response = {
                    "satname": closest_sat['satname'],
                    "sat_id": closest_sat['sat_id'],
                    "latitude": closest_sat['latitude'],
                    "longitude": closest_sat['longitude'],
                    "distance_km": round(min_dist, 2)
                }
                print(f"Sending closest satellite: {response}")
                producer.send(RESPONSE_TOPIC, value=response)

# Start streams
start_satellite_collector()

observer_parsed.writeStream \
    .foreachBatch(process_observer_requests) \
    .start() \
    .awaitTermination()
