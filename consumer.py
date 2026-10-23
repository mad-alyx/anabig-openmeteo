from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'data-cuaca',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Menunggu data cuaca...")

for message in consumer:
    data = message.value
    print("Diterima:", data)