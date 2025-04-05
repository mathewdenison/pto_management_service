import json
import time
import signal
from google.cloud import pubsub_v1
from utils.pto_update_manager import PTOUpdateManager
from utils.dashboard_events import build_dashboard_payload

import logging
from google.cloud import logging as cloud_logging

# Initialize Google Cloud logging client
client = cloud_logging.Client()
client.setup_logging()  # Automatically routes logs to Google Cloud Logging

# Now use the standard Python logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# GCP Pub/Sub subscription and topic names
project_id = "hopkinstimesheetproj"
subscription_name = "pto_deduction_sub"
dashboard_topic = "projects/hopkinstimesheetproj/topics/dashboard-queue"

# Initialize Pub/Sub clients
subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

# Subscription path
subscription_path = subscriber.subscription_path(project_id, subscription_name)

# Graceful shutdown handler
def signal_handler(sig, frame):
    """Graceful shutdown handler."""
    print('Shutting down gracefully...')
    exit(0)

# Callback function to process incoming messages
def callback(message):
    update_data = json.loads(message.data.decode("utf-8"))
    employee_id = update_data['employee_id']
    new_balance = update_data['new_balance']

    update_manager = PTOUpdateManager(employee_id, new_balance)
    result = update_manager.update_pto()

    if result['result'] == "success":
        print(f"[UPDATE] PTO for employee_id {employee_id} updated to {new_balance}")
        dashboard_payload = build_dashboard_payload(
            employee_id,
            "pto_updated",
            f"PTO for employee_id {employee_id} updated successfully.",
            {"new_pto_balance": new_balance}
        )
    else:
        print(f"[UPDATE] Failed to update PTO for employee_id {employee_id}. Reason: {result['message']}")
        dashboard_payload = build_dashboard_payload(
            employee_id,
            "pto_updated",
            f"Failed to update PTO for employee_id {employee_id}. Reason: {result['message']}"
        )

    # Publish the message to the dashboard topic
    publisher.publish(dashboard_topic, json.dumps(dashboard_payload).encode("utf-8"))
    message.ack()

# Run the subscriber to listen for messages
def run():
    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)

    # Subscribe to the PTO update queue and listen for messages
    print("Listening for messages...")
    subscriber.subscribe(subscription_path, callback=callback)

    # Keep the subscriber running indefinitely with time.sleep to avoid busy-waiting
    while True:
        time.sleep(1)  # Avoid busy-waiting and keep the process running

if __name__ == "__main__":
    run()

