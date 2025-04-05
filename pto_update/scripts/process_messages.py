import json
import time
import signal
import logging

from google.cloud import pubsub_v1
from utils.pto_update_manager import PTOUpdateManager
from utils.dashboard_events import build_dashboard_payload
from google.cloud import logging as cloud_logging

# Set up Google Cloud Logging
cloud_log_client = cloud_logging.Client()
cloud_log_client.setup_logging()

# Standard Python logging
logger = logging.getLogger("pto_deduction_worker")
logger.setLevel(logging.INFO)

# Pub/Sub setup
project_id = "hopkinstimesheetproj"
subscription_name = "pto_deduction_sub"
dashboard_topic = "projects/hopkinstimesheetproj/topics/dashboard-queue"

subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

subscription_path = subscriber.subscription_path(project_id, subscription_name)

def signal_handler(sig, frame):
    logger.info("Received shutdown signal. Exiting...")
    exit(0)

def callback(message):
    try:
        logger.info("Received new message on subscription.")
        update_data = json.loads(message.data.decode("utf-8"))
        logger.info(f"Message payload: {update_data}")

        employee_id = update_data['employee_id']
        new_balance = update_data['new_balance']

        update_manager = PTOUpdateManager(employee_id, new_balance)
        result = update_manager.update_pto()

        if result['result'] == "success":
            log_msg = f"[SUCCESS] PTO for employee_id {employee_id} updated to {new_balance}"
            logger.info(log_msg)
            dashboard_payload = build_dashboard_payload(
                employee_id,
                "pto_updated",
                log_msg,
                {"new_pto_balance": new_balance}
            )
        else:
            log_msg = f"[ERROR] Failed to update PTO for employee_id {employee_id}. Reason: {result['message']}"
            logger.error(log_msg)
            dashboard_payload = build_dashboard_payload(
                employee_id,
                "pto_updated",
                log_msg
            )

        # Send update to dashboard
        publisher.publish(dashboard_topic, json.dumps(dashboard_payload).encode("utf-8"))
        logger.info("Published update to dashboard Pub/Sub topic.")

        message.ack()
        logger.info("Message acknowledged.")
    except Exception as e:
        logger.exception(f"Error processing message: {str(e)}")
        message.nack()

def run():
    signal.signal(signal.SIGINT, signal_handler)
    logger.info("Starting PTO deduction subscriber service...")
    logger.info(f"Subscribing to: {subscription_path}")

    subscriber.subscribe(subscription_path, callback=callback)
    logger.info("Subscriber registered and listening for messages...")

    while True:
        time.sleep(60)

