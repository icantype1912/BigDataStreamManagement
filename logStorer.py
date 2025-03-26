from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
import time
from datetime import datetime

# Kafka configuration
KAFKA_BROKER = '192.168.191.129:9092'  # Replace with your Kafka broker's IP
KAFKA_TOPIC = 'service_logs'  # Replace with your topic

# Elasticsearch configuration
ELASTICSEARCH_HOST = 'http://localhost:9200'  # Default Elasticsearch URL
ES_INDEX_PREFIX = 'logs_'  # Elasticsearch index prefix for log levels

# Create an Elasticsearch client
es = Elasticsearch([ELASTICSEARCH_HOST])

# Create a Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='log_monitor',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def store_log_in_es(log, log_level):
    """Store the log in Elasticsearch under the index corresponding to the log level."""
    index_name = f"{ES_INDEX_PREFIX}{log_level.lower()}"  # Use log level as part of index name
    try:
        # Index the log into Elasticsearch
        es.index(index=index_name, document=log)
        print(f"Log stored in Elasticsearch (Index: {index_name}): {log}")
    except Exception as e:
        print(f"Error storing log in Elasticsearch: {e}")

def process_log(log):
    """Process logs and store INFO, WARN, and ERROR logs in Elasticsearch grouped by log_level."""
    log_level = log.get("log_level", "UNKNOWN")
    message_type = log.get("message_type", "UNKNOWN")

    # Only process INFO, WARN, and ERROR logs
    if log_level in ["INFO", "WARN", "ERROR"] and message_type != "HEARTBEAT":
        store_log_in_es(log, log_level)

try:
    print("Consuming messages from Kafka and storing grouped logs in Elasticsearch...")

    while True:
        for message in consumer:
            log = message.value
            process_log(log)  # Process and store the log in Elasticsearch
        time.sleep(1)  # Short delay to avoid constant polling

except KeyboardInterrupt:
    print("Stopped consuming.")
finally:
    consumer.close()
    print("Finished.")