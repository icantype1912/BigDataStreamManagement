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
    """Log an INFO level message for stock updates."""
    stock = random.choice(["AAPL", "GOOGL", "AMZN", "MSFT", "TSLA"])
    log = {
        "log_id": str(uuid.uuid4()),
        "node_id": node_id,
        "log_level": "INFO",
        "message_type": "LOG",
        "message": f"Stock {stock} updated successfully.",
        "service_name": service_name,
        "stock_symbol": stock,
        "current_price": round(random.uniform(100, 1500), 2),
        "timestamp": current_timestamp(),
        "kafka_topic": KAFKA_TOPIC
    }
    send_to_fluentd('python_service.log_info', log)

def log_warn(node_id, service_name):
    """Log a WARN level message for stock trends."""
    stock = random.choice(["AAPL", "GOOGL", "AMZN", "MSFT", "TSLA"])
    log = {
        "log_id": str(uuid.uuid4()),
        "node_id": node_id,
        "log_level": "WARN",
        "message_type": "LOG",
        "message": f"Stock {stock} volatility detected.",
        "service_name": service_name,
        "stock_symbol": stock,
        "price_change_percentage": round(random.uniform(-5, 5), 2),
        "volatility_threshold_percentage": 2.5,
        "timestamp": current_timestamp(),
        "kafka_topic": KAFKA_TOPIC
    }
    send_to_fluentd('python_service.log_warn', log)

def log_error(node_id, service_name):
    """Log an ERROR level message for stock alerts."""
    stock = random.choice(["AAPL", "GOOGL", "AMZN", "MSFT", "TSLA"])
    log = {
        "log_id": str(uuid.uuid4()),
        "node_id": node_id,
        "log_level": "ERROR",
        "message_type": "LOG",
        "message": f"Failed to fetch data for stock {stock}.",
        "service_name": service_name,
        "stock_symbol": stock,
        "error_details": {
            "error_code": "504",
            "error_message": "Gateway Timeout"
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
    service_name = "StockService"
    try:
        # Log the registration message
        log_registration(node_id, service_name)
        
        # Main loop
        while True:
            log_heartbeat(node_id)  # Log heartbeat message
            random_log(node_id, service_name)  # Log random log message
            time.sleep(1)  # Wait for 1 second
    except KeyboardInterrupt:
        # Log deregistration message when the program is interrupted
        log_deregistration(node_id, service_name, status="DOWN")

if __name__ == "__main__":
    main()

