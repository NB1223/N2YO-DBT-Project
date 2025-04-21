from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, DoubleType, StringType, LongType

# Create Spark Session with Kafka package
spark = SparkSession.builder \
    .appName("SatelliteKafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for satellite data
schema = StructType() \
    .add("sat_id", StringType()) \
    .add("satname", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("azimuth", DoubleType()) \
    .add("elevation", DoubleType()) \
    .add("timestamp", LongType())

# Read from Kafka topics using a wildcard pattern
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribePattern", "satellite-\\d+") \
    .option("startingOffsets", "latest") \
    .load()

# Extract value as string and parse JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_string", "topic") \
    .select(
        col("topic"),
        from_json(col("json_string"), schema).alias("data")
    ).select("topic", "data.*")

# Output stream to console
query = df_parsed.writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("append") \
    .start()

query.awaitTermination()
