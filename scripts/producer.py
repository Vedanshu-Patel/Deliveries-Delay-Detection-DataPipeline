import pandas as pd
import json
import time
from kafka import KafkaProducer

# 1. Setup Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 2. Read Delivery CSV
df = pd.read_csv('data\deliveries_500k.csv')

# 3. Send each row as a message
for index, row in df.iterrows():
    message = {
        'delivery_id': row['delivery_id'],
        'city': row['city'],
        'pickup_time': row['pickup_time'],
        'driver_id': row['driver_id'],
        'expected_time': row['expected_time'],
        'delivery_time': row['delivery_time']
    }
    producer.send('deliveries', value=message)
    print(f"Sent: {message}")

    time.sleep(1)  # Simulate real-time by waiting 1 second

producer.flush()
producer.close()


