from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import StructType, DoubleType, LongType, IntegerType

# Create Spark Session
spark = SparkSession.builder \
    .appName("ObserverKafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define observer location schema (including menu choice)
observer_schema = StructType() \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("altitude", DoubleType()) \
    .add("timestamp", LongType()) \
    .add("choice_id", IntegerType())

# Read only from observer_location topic
df_obs_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "observer_location") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the JSON and flatten structure
df_obs = df_obs_raw.selectExpr("CAST(value AS STRING) as json_string", "topic") \
    .select(
        col("topic"),
        from_json(col("json_string"), observer_schema).alias("data")
    ).select(
        col("topic"),
        col("data.latitude"),
        col("data.longitude"),
        col("data.altitude"),
        col("data.timestamp"),
        col("data.choice_id")
    )

# Output to console
query = df_obs.writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("append") \
    .start()

query.awaitTermination()
