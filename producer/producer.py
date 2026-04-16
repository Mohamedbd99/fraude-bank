import csv
import time
from kafka import KafkaProducer

time.sleep(20)

producer = KafkaProducer(
    bootstrap_servers="kafka:29092",
    value_serializer=lambda v: v.encode("utf-8")
)

with open("/app/data/transactions.csv", "r", encoding="utf-8") as file:
    reader = csv.reader(file)
    next(reader)

    for row in reader:
        message = ",".join(row)
        producer.send("transactions", message)
        print("Sent:", message, flush=True)
        time.sleep(0.5)

producer.flush()
print("Finished sending all transactions.", flush=True) 