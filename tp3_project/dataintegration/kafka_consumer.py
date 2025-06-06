# kafka_consumer.py
from confluent_kafka import Consumer, KafkaException
import os, json, sqlite3
import pandas as pd
from datetime import datetime

TOPICS = ['TRANSACTION_STATUS_COUNTS', 'ANONYME_TRANSACTIONS', 'BLACKLIST_TRANSACTIONS', 'ANONYME_TRANSACTIONS_PROPRE', 'MOYENNE_TRANSACTIONS_IMPORTANTES']
STREAM_TOPICS = ['ANONYME_TRANSACTIONS', 'BLACKLIST_TRANSACTIONS', 'MOYENNE_TRANSACTIONS_IMPORTANTES']
TABLE_TOPICS = ['TRANSACTION_STATUS_COUNTS', 'ANONYME_TRANSACTIONS_PROPRE']

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'data-lake-writer',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)

def consume_to_datalake(topic):
    consumer.subscribe([topic])
    data = []
    for _ in range(50):
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        try:
            val = json.loads(msg.value().decode('utf-8'))
            data.extend(val if isinstance(val, list) else [val])
        except:
            continue

    df = pd.DataFrame(data)
    today = datetime.now().strftime('%Y-%m-%d')
    path = f"C:/Users/couta/Python project/data/data_lake/{topic}/date={today}"
    os.makedirs(path, exist_ok=True)
    file_path = os.path.join(path, f"part-{today}.csv")

    mode = 'a' if topic in STREAM_TOPICS else 'w'
    df.to_csv(file_path, mode=mode, header=not os.path.exists(file_path), index=False)

def consume_to_warehouse(db_path, max_messages=100):
    consumer.subscribe(TOPICS)
    def insert(data):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        if 'transaction_id' in data:
            cursor.execute('''INSERT INTO anonyme_transactions_propre (...) VALUES (...)''', (...))
        elif 'TOTAL_TRANSACTIONS' in data:
            cursor.execute('''INSERT INTO transaction_status_counts (...) VALUES (...)''', (...))
        conn.commit()
        conn.close()

    count = 0
    while count < max_messages:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        try:
            data = json.loads(msg.value().decode('utf-8'))
            insert(data)
            count += 1
        except:
            continue


def preview_messages(topic_name, max_messages=100):
    conf_preview = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': f'preview-reader-{topic_name}',
        'auto.offset.reset': 'earliest'
    }

    preview_consumer = Consumer(conf_preview)
    preview_consumer.subscribe([topic_name])
    messages = []

    try:
        while len(messages) < max_messages:
            msg = preview_consumer.poll(1.0)
            if msg is None:
                break
            if msg.error():
                continue
            try:
                data = json.loads(msg.value().decode('utf-8'))
                messages.append(data)
            except Exception as e:
                continue
    finally:
        preview_consumer.close()

    return messages
