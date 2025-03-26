import uuid
import random
import json
import time
from datetime import datetime
import requests

# Fluentd and Kafka setup
FLUENTD_HOST = 'http://localhost:9880'
KAFKA_TOPIC = 'service_logs'

def current_timestamp():
    """Get the current timestamp in ISO 8601 format."""
    return datetime.now().isoformat()

def send_to_fluentd(tag, log):
    """Send logs to Fluentd."""
    url = f"{FLUENTD_HOST}/{tag}"
    try:
        response = requests.post(url, json=log, timeout=2)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to send log to Fluentd: {e}")

def log_registration(node_id, service_name):
    """Log the registration message."""
    log = {
        "node_id": node_id,
        "message_type": "REGISTRATION",
        "service_name": service_name,
        "timestamp": current_timestamp(),
        "kafka_topic": KAFKA_TOPIC
    }
    send_to_fluentd('python_service.registration', log)

def log_deregistration(node_id, service_name, status):
    """Log the deregistration message."""
    log = {
        "message_type": "DEREGISTRATION",
        "node_id": node_id,
        "service_name": service_name,
        "status": status,
        "timestamp": current_timestamp(),
        "kafka_topic": KAFKA_TOPIC
    }
    send_to_fluentd('python_service.deregistration', log)

def log_heartbeat(node_id):
    """Log the heartbeat message."""
    log = {
        "node_id": node_id,
        "message_type": "HEARTBEAT",
        "status": "UP",
        "timestamp": current_timestamp(),
        "kafka_topic": KAFKA_TOPIC
    }
    send_to_fluentd('python_service.heartbeat', log)

def log_info(node_id, service_name):
    """Log an INFO level message."""
    log = {
        "log_id": str(uuid.uuid4()),
        "node_id": node_id,
        "log_level": "INFO",
        "message_type": "LOG",
        "message": "Billing processed successfully.",
        "service_name": service_name,
        "timestamp": current_timestamp(),
        "kafka_topic": KAFKA_TOPIC
    }
    send_to_fluentd('python_service.log_info', log)

def log_warn(node_id, service_name):
    """Log a WARN level message."""
    log = {
        "log_id": str(uuid.uuid4()),
        "node_id": node_id,
        "log_level": "WARN",
        "message_type": "LOG",
        "message": "Billing response time is nearing the threshold.",
        "service_name": service_name,
        "response_time_ms": random.randint(300, 1200),
        "threshold_limit_ms": 800,
        "timestamp": current_timestamp(),
        "kafka_topic": KAFKA_TOPIC
    }
    send_to_fluentd('python_service.log_warn', log)

def log_error(node_id, service_name):
    """Log an ERROR level message."""
    log = {
        "log_id": str(uuid.uuid4()),
        "node_id": node_id,
        "log_level": "ERROR",
        "message_type": "LOG",
        "message": "Failed to process billing request.",
        "service_name": service_name,
        "error_details": {
            "error_code": "PAY-502",
            "error_message": "Payment Gateway Error"
        },
        "timestamp": current_timestamp(),
        "kafka_topic": KAFKA_TOPIC
    }
    send_to_fluentd('python_service.log_error', log)

def random_log(node_id, service_name):
    """Randomly log one of the three log types: INFO, WARN, or ERROR."""
    log_functions = [log_info, log_warn, log_error]
    random.choice(log_functions)(node_id, service_name)

def main():
    """Main function to run the microservice."""
    node_id = str(uuid.uuid4())
    service_name = "BillingService"
    try:
        log_registration(node_id, service_name)
        while True:
            log_heartbeat(node_id)
            random_log(node_id, service_name)
            time.sleep(1)
    except KeyboardInterrupt:
        log_deregistration(node_id, service_name, status="DOWN")

if __name__ == "__main__":
    main()
