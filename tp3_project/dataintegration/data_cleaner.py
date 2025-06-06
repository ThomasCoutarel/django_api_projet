# data_cleaner.py
import os, shutil, sqlite3
from datetime import datetime
import pandas as pd

def clean_old_data_lake(base_path, topics, retention_days=30):
    now = datetime.now()
    limit = now - pd.Timedelta(days=retention_days)
    for topic in topics:
        topic_path = os.path.join(base_path, topic)
        if not os.path.exists(topic_path):
            continue
        for folder in os.listdir(topic_path):
            try:
                folder_date = datetime.strptime(folder.split('=')[1], '%Y-%m-%d')
                if folder_date < limit:
                    shutil.rmtree(os.path.join(topic_path, folder))
            except:
                continue

def clean_sqlite_data(db_path, retention_days=30):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    limit = (datetime.now() - pd.Timedelta(days=retention_days)).strftime('%Y-%m-%d %H:%M:%S')
    try:
        cursor.execute("DELETE FROM anonyme_transactions_propre WHERE timestamp_of_reception_log < ?", (limit,))
        cursor.execute("DELETE FROM transaction_status_counts WHERE last_updated < ?", (limit,))
        conn.commit()
    finally:
        conn.close()
