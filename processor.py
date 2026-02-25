import json
import statistics  # Added for Standard Deviation math
from confluent_kafka import Consumer, Producer
import os
# Configs


kafka_broker = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_CONF = {'bootstrap.servers': kafka_broker}
CONSUMER_CONF = {**KAFKA_CONF, 'group.id': 'silver-processor-group', 'auto.offset.reset': 'earliest'}

consumer = Consumer(CONSUMER_CONF)
producer = Producer(KAFKA_CONF)

consumer.subscribe(['sensor_telemetry'])

# Tracking history for stats
sensor_history = {'sensor_00': [], 'sensor_01': []}

print("Silver Processor started. Detecting statistical anomalies (Z-Score)...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        
        try:
            data = json.loads(msg.value().decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            continue
        
        for s_name in ['sensor_00', 'sensor_01']:
            val = data.get(s_name)
            if val is not None:
                val = float(val)
                history = sensor_history[s_name]

                # --- STATISTICAL LOGIC START ---
                # We need at least 2 points to calculate standard deviation
                if len(history) >= 5:
                    avg = statistics.mean(history)
                    std_dev = statistics.stdev(history)
                    
                    # Threshold: 3 Standard Deviations (The 3-Sigma Rule)
                    # If std_dev is 0 (all same values), we use a tiny floor value to avoid division by zero
                    threshold = avg + (3 * max(std_dev, 0.1))

                    if val > threshold:
                        alert = {
                            "time": data.get('timestamp'),
                            "sensor": s_name,
                            "value": val,
                            "average": round(avg, 2),
                            "std_dev": round(std_dev, 2),
                            "message": f"Statistical Anomaly: {val} is > 3 sigma from mean."
                        }
                        print(f"🚨 ALERT: {s_name} Outlier! Val: {val} | Mean: {round(avg, 2)} | StdDev: {round(std_dev, 2)}")
                        producer.produce('sensor_alerts', value=json.dumps(alert))
                # --- STATISTICAL LOGIC END ---

                # Update history for the NEXT message
                history.append(val)
                if len(history) > 20: # Increased window slightly for better stats
                    history.pop(0)

        producer.poll(0)

except KeyboardInterrupt:
    pass
finally:
    producer.flush() # Ensure final alerts are sent
    consumer.close()