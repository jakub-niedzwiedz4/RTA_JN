from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='category-stats-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# lambda stworzy startowy słownik dla każdej nowej kategorii
category_stats = defaultdict(lambda: {
    'count': 0,
    'revenue': 0.0,
    'min': float('inf'),
    'max': float('-inf')
})

msg_count = 0

print("Nasłuchiwanie transakcji i obliczanie statystyk per kategoria...")

for message in consumer:
    tx = message.value
    category = tx['category']
    amount = tx['amount']
    
    # aktualizacja stanu dla konkretnej kategorii
    stats = category_stats[category]
    stats['count'] += 1
    stats['revenue'] += amount
    
    # sprawdzanie min i max
    if amount < stats['min']:
        stats['min'] = amount
    if amount > stats['max']:
        stats['max'] = amount
    
    msg_count += 1
    
    # co 10 wiadomosci wypisz tabele ze statystykami
    if msg_count % 10 == 0:
        print(f"\n[ Raport po {msg_count} wiadomościach ]")
        header = f"{'Kategoria':<15} | {'Liczba':<6} | {'Suma':<10} | {'Min':<8} | {'Max':<8}"
        print(header)
        print("-" * len(header))
        
        # sortowanie po nazwie kategorii dla czytelnosci
        for cat in sorted(category_stats.keys()):
            s = category_stats[cat]
            print(f"{cat:<15} | {s['count']:<6} | {s['revenue']:>10.2f} | {s['min']:>8.2f} | {s['max']:>8.2f}")
        print("-" * len(header))
