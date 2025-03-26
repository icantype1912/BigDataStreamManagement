import uuid
import random
import json
import time
from datetime import datetime


def current_timestamp():
    """Get the current timestamp in ISO 8601 format."""
    return datetime.now().isoformat()


def log_registration(node_id, service_name):
    """Log the registration message."""
    log = {
        "node_id": node_id,
        "message_type": "REGISTRATION",
        "service_name": service_name,
        "timestamp": current_timestamp(),
    }
    print("Microservice Registration Message")
    print(json.dumps(log, indent=4))


def log_deregistration(node_id, service_name, status):
    """Log the deregistration message."""
    log = {
        "message_type": "REGISTRATION",
        "node_id": node_id,
        "service_name": service_name,
        "status": status,
        "timestamp": current_timestamp(),
    }
    print(json.dumps(log, indent=4))


def log_heartbeat(node_id):
    """Log the heartbeat message."""
    log = {
        "node_id": node_id,
        "message_type": "HEARTBEAT",
        "status": "UP",
        "timestamp": current_timestamp(),
    }
    print("Heartbeat Message")
    print(json.dumps(log, indent=4))


def log_info(node_id, service_name):
    """Log an INFO level message."""
    log = {
        "log_id": str(uuid.uuid4()),
        "node_id": node_id,
        "log_level": "INFO",
        "message_type": "LOG",
        "message": "Service running smoothly.",
        "service_name": service_name,
        "timestamp": current_timestamp(),
    }
    print("INFO Log")
    print(json.dumps(log, indent=4))


def log_warn(node_id, service_name):
    """Log a WARN level message."""
    log = {
        "log_id": str(uuid.uuid4()),
        "node_id": node_id,
        "log_level": "WARN",
        "message_type": "LOG",
        "message": "Response time approaching threshold.",
        "service_name": service_name,
        "response_time_ms": random.randint(200, 1000),
        "threshold_limit_ms": 500,
        "timestamp": current_timestamp(),
    }
    print("WARN Log")
    print(json.dumps(log, indent=4))


def log_error(node_id, service_name):
    """Log an ERROR level message."""
    log = {
        "log_id": str(uuid.uuid4()),
        "node_id": node_id,
        "log_level": "ERROR",
        "message_type": "LOG",
        "message": "Failed to fetch weather data.",
        "service_name": service_name,
        "error_details": {
            "error_code": "503",
            "error_message": "Service Unavailable"
        },
        "timestamp": current_timestamp(),
    }
    print("ERROR Log")
    print(json.dumps(log, indent=4))


def random_log(node_id, service_name):
    """Randomly log one of the three log types: INFO, WARN, or ERROR."""
    log_functions = [log_info, log_warn, log_error]
    random.choice(log_functions)(node_id, service_name)


def main():
    """Main function to run the microservice."""
    node_id = str(uuid.uuid4())
    service_name = "NotificationService"

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
