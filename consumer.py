import json
from kafka import KafkaConsumer

BROKER   = "localhost:909"
TOPIC    = "greeting"
GROUP_ID = "my-consumer-group"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BROKER,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)
print(f"Listening on topic '{TOPIC}' - Ctrl+C to stop\n")
try:
    for msg in consumer:
        print(f"  Received <- {msg.value} [partition={msg.partition}, offset={msg.offset}]")
except KeyboardInterrupt:
    print("\nConsumer stopped.")
finally:
    consumer.close()
