import json, time
from kafka import KafkaProducer

BROKER = "localhost:9091"
TOPIC  = "greetings"

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
print(f"Connected! Sending to topic '{TOPIC}' - Ctrl+C to stop\n")
count = 1
try:
    while True:
        msg = {"id": count, "text": f"Hello from message #{count}"}
        meta = producer.send(TOPIC, value=msg).get(timeout=10)
        print(f"  Sent -> {msg} [partition={meta.partition}, offset={meta.offset}]")
        count += 1
        time.sleep(1)
except KeyboardInterrupt:
    print("\nProducer stopped.")
finally:
    producer.close()
