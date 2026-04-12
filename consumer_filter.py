from kafka import KafkaConsumer
import json

# konfiguracja konsumenta
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', # earliest- zacznie czytac od poczatku
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Nasłuchuję na duże transakcje (amount > 3000)...")

for message in consumer:
    tx = message.value
    
    if tx['amount'] > 3000:
        # Format: ALERT: TX0042 | 2345.67 PLN | Warszawa | elektronika
        print(f"ALERT: {tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']} | {tx['category']}")
