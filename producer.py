import pandas as pd
import json
import time
import logging
import datetime
from confluent_kafka import Producer
import os

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Use 'kafka:9092' when in Docker, default to 'localhost:9092' for local testing
kafka_broker = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
conf = {'bootstrap.servers': kafka_broker}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        # We don't want to print every single message in production, 
        # but for learning, it's great to see it working.
        pass

def run_producer(file_path):
    logger.info("Loading dataset...")
    df = pd.read_csv(file_path)
    
    # 1. CLEANING: Remove completely empty/useless columns
    # Unnamed: 0 is just a row index. sensor_15 is 100% null.
    df = df.drop(columns=['Unnamed: 0', 'sensor_15'], errors='ignore')
    
    # 2. PREP: Convert to datetime and sort
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')
    
    logger.info(f"Starting stream replay for {len(df)} rows...")
    last_timestamp = None

    for _, row in df.iterrows():
        # --- DATA QUALITY LOGIC ---
        # If sensor_00 is null, we'll mark the record as 'UNCERTAIN'
        quality = 'GOOD'
        if pd.isna(row['sensor_00']):
            quality = 'UNCERTAIN'
        
        # Convert row to dictionary and handle NaNs for JSON compatibility
        # JSON cannot handle 'NaN', so we convert them to None (which becomes null)
        record = row.where(pd.notnull(row), None).to_dict()
        #record['timestamp'] = record['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
        record['timestamp'] = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        record['data_quality_flag'] = quality

        # --- REPLAY SIMULATION ---
        current_timestamp = row['timestamp']
        if last_timestamp is not None:
            # Replay at 100x speed (1 minute of real time = 0.6 seconds of wait)
            wait = (current_timestamp - last_timestamp).total_seconds() / 100
            time.sleep(wait)
        
        # --- SEND TO KAFKA ---
        # Topic: 'sensor_telemetry'
        # Key: The machine status (Normal/Broken) - helps with partitioning later
        producer.produce(
            'sensor_telemetry',
            key=str(record['machine_status']),
            value=json.dumps(record),
            callback=delivery_report
        )
        
        # Poll handles the callbacks from the 'delivery_report'
        producer.poll(0)
        last_timestamp = current_timestamp

    producer.flush()

if __name__ == "__main__":
    run_producer('sensor.csv')