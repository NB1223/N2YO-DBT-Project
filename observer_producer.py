import time
import json
from kafka import KafkaProducer, KafkaConsumer

# Kafka config
KAFKA_BROKER = 'localhost:9092'
OBSERVER_TOPIC = 'observer_location'
RESPONSE_TOPIC = 'observer_response'

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Kafka Consumer
consumer = KafkaConsumer(
    RESPONSE_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id="observer_waiter",
    auto_offset_reset="latest",
    enable_auto_commit=True
)

# Shared observer state
observer_data = {
    "latitude": None,
    "longitude": None,
    "altitude": None,
    "timestamp": None,
    "choice_id": None
}

def get_location():
    while True:
        try:
            print("\nEnter Observer Location:")
            observer_data["latitude"] = float(input("Latitude: "))
            observer_data["longitude"] = float(input("Longitude: "))
            observer_data["altitude"] = float(input("Altitude (m): "))
            break
        except ValueError:
            print("Invalid input. Please enter numeric values.")

def show_menu():
    print("\nMenu Options:")
    print("1. Motion Vector Calculation")
    print("2. Coverage Overlap")
    print("3. Closest Satellite to Observer")
    print("4. Exit")

def get_menu_choice():
    while True:
        show_menu()
        choice = input("Choose an option (1-4): ").strip()
        if choice in {'1', '2', '3'}:
            observer_data["choice_id"] = int(choice)
            return
        elif choice == '4':
            print("Exiting observer interface.")
            exit(0)
        else:
            print("Invalid choice. Try again.")

def send_to_kafka_and_wait():
    observer_data["timestamp"] = int(time.time())
    print(f"\nSending to Kafka: {observer_data}")
    producer.send(OBSERVER_TOPIC, value=observer_data)

    print("Waiting for response...")
    for msg in consumer:
        response = msg.value
        print("\nResponse received:")
        for key, value in response.items():
            print(f"{key}: {value}")
        break

def main_loop():
    while True:
        get_menu_choice()
        if observer_data["choice_id"] == 3:
            get_location()
            send_to_kafka_and_wait()
        else:
            print(f"Option {observer_data['choice_id']} selected - functionality not implemented.")

if __name__ == "__main__":
    print("Observer Kafka Producer Started")
    main_loop()
