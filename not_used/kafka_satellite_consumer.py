from kafka import KafkaConsumer
import json
import re

#file not being used


# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_PATTERN = re.compile(r'^satellite-\d+$')  # Match topics like satellite-49810

# Initialize consumer (subscribe to all topics)
consumer = KafkaConsumer(
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='latest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    consumer_timeout_ms=1000
)

# Get initial list of topics and filter by pattern
all_topics = consumer.topics()
satellite_topics = [topic for topic in all_topics if TOPIC_PATTERN.match(topic)]
consumer.subscribe(topics=satellite_topics)

print("Listening to satellite Kafka channels...\nPress Ctrl+C to stop.\n")

try:
    while True:
        for msg in consumer.poll(timeout_ms=1000).values():
            for record in msg:
                payload = record.value
                print(f"[Topic: {record.topic}] Satellite ID: {payload.get('sat_id')}")
                print(f"    ➤ Name      : {payload.get('satname')}")
                print(f"    ➤ Latitude  : {payload.get('latitude')}")
                print(f"    ➤ Longitude : {payload.get('longitude')}")
                print(f"    ➤ Azimuth   : {payload.get('azimuth')}")
                print(f"    ➤ Elevation : {payload.get('elevation')}")
                print(f"    ➤ Timestamp : {payload.get('timestamp')}\n")

except KeyboardInterrupt:
    print("\nStopped by user.")

finally:
    consumer.close()
