from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sklepy = ['Warszawa', 'Kraków', 'Gdańsk', 'Wrocław']
kategorie = ['elektronika', 'odzież', 'żywność', 'książki']

def generate_transaction(counter):
    return {
        # Formatowanie TX0001, TX0002 itd.
        'tx_id': f'TX{counter:04d}', 
        'user_id': f'u{random.randint(1,20):02d}',
        'amount': round(random.uniform(5.0, 5000.0), 2),
        'store': random.choice(sklepy),
        'category': random.choice(kategorie),
        'timestamp': datetime.now().isoformat(),
    }

for i in range(1, 1001):
    tx = generate_transaction(i)
    producer.send('transactions', value=tx)
    print(f"[{i}] Wysłano: {tx['tx_id']} | {tx['amount']} PLN")
    
    time.sleep(1) 

producer.flush()
producer.close()
