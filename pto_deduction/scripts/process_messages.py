import json
import time
import logging
import threading
import signal

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

# Event for graceful shutdown
shutdown_event = threading.Event()

def signal_handler(sig, frame):
    logger.info("Received shutdown signal. Preparing to exit...")
    shutdown_event.set()

def callback(message):
    try:

        logger.info("Received new message on subscription.")
        raw_data = message.data.decode("utf-8")
        logger.info(f"Raw message received: {raw_data}")

        # First decode
        update_data = json.loads(raw_data)
        # If update_data is a string (i.e. still JSON-encoded), decode it again.
        if isinstance(update_data, str):
            update_data = json.loads(update_data)
        logger.info(f"Message payload: {update_data}")

        employee_id = update_data['employee_id']
        pto_deduction = update_data['pto_deduction']  # Deduction value provided in the message.

        # Create an instance of PTOUpdateManager for the given employee.
        update_manager = PTOUpdateManager(employee_id)

        # Use the subtract_pto method to subtract the deduction.
        result = update_manager.subtract_pto(pto_deduction)

        # Retrieve the updated balance (if needed).
        new_balance = update_manager.get_current_balance() if hasattr(update_manager, 'get_current_balance') else "unknown"

        if result['result'] == "success":
            log_msg = f"[SUCCESS] PTO for employee {employee_id} updated. New balance: {new_balance}"
            logger.info(log_msg)
            dashboard_payload = build_dashboard_payload(
                employee_id,
                "refresh_data",
                "Please refresh dashboard data.",
                {}
            )
        else:
            log_msg = f"[ERROR] Failed to update PTO for employee {employee_id}. Reason: {result['message']}"
            logger.error(log_msg)
            dashboard_payload = build_dashboard_payload(
                employee_id,
                "pto_updated",
                log_msg
            )

        # Publish the dashboard payload to the dashboard topic.
        publisher.publish(dashboard_topic, json.dumps(dashboard_payload).encode("utf-8"))
        logger.info("Published update to dashboard Pub/Sub topic.")

        message.ack()
        logger.info("Message acknowledged.")
    except Exception as e:
        logger.exception(f"Error processing message: {str(e)}")
        message.nack()



def run():
    if threading.current_thread() == threading.main_thread():
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    logger.info(f"Subscribing to {subscription_path}")
    future = subscriber.subscribe(subscription_path, callback=callback)
    logger.info("Waiting for PTO deduction messages...")

    try:
        while not shutdown_event.is_set():
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received.")
        shutdown_event.set()
    finally:
        future.cancel()
        subscriber.close()
        logger.info("Shutdown complete. Subscriber closed.")
