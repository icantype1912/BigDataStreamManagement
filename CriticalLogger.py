from kafka import KafkaConsumer
import json
import time
from datetime import datetime

# Kafka configuration
KAFKA_BROKER = '192.168.191.129:9092'  
KAFKA_TOPIC = 'service_logs'  

# Heartbeat monitoring configuration
HEARTBEAT_TIMEOUT = 10  # Seconds to wait before considering a node as failed
HEARTBEAT_FILE = "heartbeat_status.json"  # File to store heartbeat timestamps

# ANSI color codes for making output pop
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"  # Blue color for heartbeat failure alerts
GREEN = "\033[32m"  # Green color for registration logs
PINK = "\033[38;5;204m"  # Pink color for deregistration logs
RESET = "\033[0m"

# Create a Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='log_monitor',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Track the last heartbeat timestamp for each node
last_heartbeat = {}


def process_log(log):
    """Process and print WARN, ERROR, REGISTRATION, and DEREGISTRATION logs."""
    log_level = log.get("log_level", "UNKNOWN")
    node_id = log.get("node_id")

    if log_level in ["WARN", "ERROR"]:
        color = YELLOW if log_level == "WARN" else RED
        print(f"{color}[{datetime.now()}] {log_level} Log - Node {node_id}: {log.get('message')}{RESET}")

    # Handle REGISTRATION logs
    elif log.get("message_type") == "REGISTRATION":
        color = GREEN
        print(
            f"{color}[{datetime.now()}] REGISTRATION - Node {node_id} registered for {log.get('service_name')}{RESET}")

    # Handle DEREGISTRATION logs
    elif log.get("message_type") == "DEREGISTRATION":
        color = PINK
        print(
            f"{color}[{datetime.now()}] DEREGISTRATION - Node {node_id} deregistered for {log.get('service_name')} with status {log.get('status')}{RESET}")


def update_heartbeat(log):
    """Update the heartbeat timestamp for the node."""
    if log.get("message_type") == "HEARTBEAT":
        node_id = log.get("node_id")
        timestamp = log.get("timestamp")  # Use the timestamp from the heartbeat log
        last_heartbeat[node_id] = timestamp


def check_node_failure():
    """Check for any nodes that haven't sent a heartbeat in the allowed time."""
    now = datetime.now()
    failed_nodes = []

    # Check each node's heartbeat timestamp
    for node_id, timestamp in last_heartbeat.items():
        last_time = datetime.fromisoformat(timestamp)
        if (now - last_time).total_seconds() > HEARTBEAT_TIMEOUT:
            failed_nodes.append(node_id)

    # Alert for failed nodes with blue color
    for node_id in failed_nodes:
        print(f"{BLUE}[{now}] ALERT: Node {node_id} is considered as FAILED due to lack of heartbeat!{RESET}")
        del last_heartbeat[node_id]  # Remove the failed node from tracking


try:
    print("Consuming messages from Kafka and monitoring heartbeats...")

    while True:
        for message in consumer:
            log = message.value
            process_log(log)  # Process WARN, ERROR, REGISTRATION, and DEREGISTRATION logs
            update_heartbeat(log)  # Update heartbeat timestamps
            check_node_failure()

        check_node_failure()  # Monitor for failed nodes
        time.sleep(1)  # Short delay to avoid constant polling

except KeyboardInterrupt:
    print("Stopped consuming.")
finally:
    consumer.close()
    print("Finished.")
