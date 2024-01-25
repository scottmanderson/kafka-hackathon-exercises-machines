import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "machines",
    bootstrap_servers='localhost:9092',
    max_poll_records = 100,
    key_deserializer=lambda m: json.loads(m.decode('ascii')),
    value_deserializer=lambda m: json.loads(m.decode('ascii'))
)

for message in consumer:
    print(json.loads(message.value))
