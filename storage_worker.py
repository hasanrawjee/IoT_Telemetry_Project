import json
import sqlite3
from confluent_kafka import Consumer
import os

# 1. Database Setup
DB_NAME = "factory_alerts.db"

def init_db():
    """Creates the database and table if they don't exist."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS vibration_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            sensor_name TEXT,
            value REAL,
            average REAL,
            message TEXT
        )
    ''')
    conn.commit()
    return conn

# 2. Kafka Config

kafka_broker = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
CONF = {
    'bootstrap.servers': kafka_broker,
    'group.id': 'gold-storage-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(CONF)
consumer.subscribe(['sensor_alerts'])

def save_to_gold():
    print(f"📦 Gold Storage Worker started. Saving alerts to {DB_NAME}...")
    db_conn = init_db()
    cursor = db_conn.cursor()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            # Parse the alert from Silver Layer
            alert_data = json.loads(msg.value().decode('utf-8'))
            
            # 3. Insert into SQLite
            cursor.execute('''
                INSERT INTO vibration_alerts (timestamp, sensor_name, value, average, message)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                alert_data.get('time'),
                alert_data.get('sensor'),
                alert_data.get('value'),
                alert_data.get('average'),
                alert_data.get('message')
            ))
            
            db_conn.commit()
            print(f"✅ Alert persisted to DB: {alert_data.get('sensor')} at {alert_data.get('time')}")

    except KeyboardInterrupt:
        pass
    finally:
        db_conn.close()
        consumer.close()

if __name__ == "__main__":
    save_to_gold()