from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    # inne group_id, żeby ten konsument działał niezależnie od pierwszego
    group_id='enricher-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Uruchomiono wzbogacanie transakcji o poziom ryzyka...")

for message in consumer:
    tx = message.value
    amount = tx.get('amount', 0)
    
    # logika poziomu ryzyka
    if amount > 3000:
        risk = "HIGH"
    elif amount > 1000:
        risk = "MEDIUM"
    else:
        risk = "LOW"
        
    # dodanie risk_level do słownika
    tx['risk_level'] = risk
    
    print(f"[{risk}] TX: {tx['tx_id']} | Kwota: {amount:.2f} | Sklep: {tx['store']}")
