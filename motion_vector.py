from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit, when
from pyspark.sql.types import StructType, DoubleType, StringType, LongType, IntegerType
from kafka import KafkaProducer
from pyspark.sql import Row
# from datetime import datetime, timedelta
import json
import math
# from Calculations.coverage_overlap import calculate_overlap


response_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

spark = SparkSession.builder \
    .appName("SatelliteKafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")


satellite_schema = StructType() \
    .add("sat_id", StringType()) \
    .add("satname", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("azimuth", DoubleType()) \
    .add("elevation", DoubleType()) \
    .add("timestamp", LongType())

observer_schema = StructType() \
    .add("obs_latitude", DoubleType()) \
    .add("obs_longitude", DoubleType()) \
    .add("altitude", DoubleType()) \
    .add("timestamp", LongType()) \
    .add("choice_id", IntegerType())


df_raw_sat = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribePattern", "satellite-\\d+") \
    .option("startingOffsets", "latest") \
    .load()

df_sat = df_raw_sat.selectExpr("CAST(value AS STRING) as json_string", "topic") \
    .select(from_json(col("json_string"), satellite_schema).alias("data")) \
    .select("data.*")


df_raw_obs = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "observer_location") \
    .option("startingOffsets", "latest") \
    .load()

df_obs = df_raw_obs.selectExpr("CAST(value AS STRING) as json_string", "topic") \
    .select(from_json(col("json_string"), observer_schema).alias("data")) \
    .select("data.*")


df_with_choice = df_obs.withColumn(
    "action",
    when(col("choice_id") == 1, lit("motion_vector"))
    .when(col("choice_id") == 2, lit("overlap"))
    .when(col("choice_id") == 3, lit("closest"))
    .when(col("choice_id") == 4, lit("exit"))
    .otherwise(lit("unknown"))
)

satellite_cache = {}
last_position = {}


def update_satellite_cache(new_data):
    sat_id = new_data['sat_id']
    satellite_cache[sat_id] = new_data

def get_cached_satellites():
    return [Row(**data) for data in satellite_cache.values()]

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2.0)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2.0)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def calculate_motion_vector(prev, curr):
    lat1, lon1, t1 = prev['latitude'], prev['longitude'], prev['timestamp']
    lat2, lon2, t2 = curr['latitude'], curr['longitude'], curr['timestamp']
    distance = haversine(lat1, lon1, lat2, lon2)
    time_diff = t2 - t1
    if time_diff == 0:
        return 0, (0, 0)
    velocity = distance / (time_diff / 3600)
    direction = (lat2 - lat1, lon2 - lon1)
    return velocity, direction

position_history = {}

def process_satellite_data(batch_df, batch_id):
    rows = batch_df.collect()
    response_list = []

    for row in rows:
        row_dict = row.asDict()
        update_satellite_cache(row_dict)
        sat_id = row_dict['sat_id']
        current = {
            "latitude": row_dict["latitude"],
            "longitude": row_dict["longitude"],
            "timestamp": row_dict["timestamp"]
        }

        now_ts = row_dict["timestamp"]

        
        if sat_id not in position_history:
            position_history[sat_id] = []
        position_history[sat_id].append(current)

        
        position_history[sat_id] = [
            pos for pos in position_history[sat_id] 
            if now_ts - pos["timestamp"] <= 300
        ]

        
        if len(position_history[sat_id]) >= 2:
            prev = position_history[sat_id][0]
            velocity, direction = calculate_motion_vector(prev, current)

            response = {
                "sat_id": sat_id,
                "satname": row_dict["satname"],
                "velocity_kmph": round(velocity, 2),
                "direction": {
                    "delta_latitude": round(direction[0], 5),
                    "delta_longitude": round(direction[1], 5)
                },
                "timestamp": now_ts
            }
            print(f"[MOTION VECTOR - 5 MIN] {response}")
            response_list.append(response)

    
    if response_list:
        batch_to_send = response_list[:5] 
        response_producer.send("motion_vector_response", value=batch_to_send)


query_sat = df_sat.writeStream \
    .foreachBatch(process_satellite_data) \
    .option("checkpointLocation", "/tmp/spark_checkpoint_sat") \
    .start()


query_sat.awaitTermination()
