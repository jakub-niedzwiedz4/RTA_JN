from kafka import KafkaConsumer
import json
from collections import defaultdict
from datetime import datetime, timedelta

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='latest',
    group_id='anomaly-detector-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_windows = defaultdict(list)

print("System detekcji anomalii prędkości uruchomiony (60s okno)...")

for message in consumer:
    tx = message.value
    u_id = tx['user_id']
    now = datetime.fromisoformat(tx['timestamp'])
    
    # dodaje bieżącą transakcję do okna użytkownika
    user_windows[u_id].append(now)
    
    # usuwa z okna transakcje starsze niż 60 sekund
    one_minute_ago = now - timedelta(seconds=60)
    user_windows[u_id] = [ts for ts in user_windows[u_id] if ts > one_minute_ago]
    
    # sprawdza warunek anomalii
    tx_count = len(user_windows[u_id])
    if tx_count > 3:
        print(f"!!! ALARM ANOMALII !!!")
        print(f"Użytkownik: {u_id} wykonał {tx_count} transakcje w ciągu ostatniej minuty!")
        print(f"Ostatnia transakcja: {tx['tx_id']} | Kwota: {tx['amount']} PLN")
        print("-" * 30)
