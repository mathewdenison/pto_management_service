import json
import time
import logging
import threading
import signal

from google.cloud import pubsub_v1
from utils.pto_update_manager import PTOUpdateManager
from utils.dashboard_events import build_dashboard_payload
from google.cloud import logging as cloud_logging

# Google Cloud Logging setup
cloud_log_client = cloud_logging.Client()
cloud_log_client.setup_logging()

# Standard Python logging
logger = logging.getLogger("pto_update_worker")
logger.setLevel(logging.INFO)

# GCP configuration
project_id = "hopkinstimesheetproj"
subscription_name = "pto_update_processing_sub"
dashboard_topic = f"projects/{project_id}/topics/dashboard-queue"

subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

# Shutdown event
shutdown_event = threading.Event()

def signal_handler(sig, frame):
    logger.info("Received shutdown signal. Preparing to exit...")
    shutdown_event.set()

def callback(message):
    try:
        logger.info("Received new message on subscription.")
        raw_data = message.data.decode("utf-8")
        logger.info(f"Raw message received: {raw_data}")

        update_data = json.loads(raw_data)
        if isinstance(update_data, str):
            update_data = json.loads(update_data)

        employee_id = update_data['employee_id']
        new_balance = update_data['new_balance']

        update_manager = PTOUpdateManager(employee_id, new_balance)
        result = update_manager.update_pto()

        if result['result'] == "success":
            msg = f"[SUCCESS] PTO for employee_id {employee_id} updated to {new_balance}"
            logger.info(msg)
            dashboard_payload = build_dashboard_payload(
                employee_id, "refresh_data", "Time log created, please refresh dashboard data."
            )
        else:
            msg = f"[ERROR] Failed to update PTO for employee_id {employee_id}. Reason: {result['message']}"
            logger.error(msg)
            dashboard_payload = build_dashboard_payload(employee_id, "pto_updated", msg)

        publisher.publish(dashboard_topic, json.dumps(dashboard_payload).encode("utf-8"))
        logger.info("Published update to dashboard Pub/Sub topic.")
        message.ack()
        logger.info("Message acknowledged.")

    except Exception as e:
        logger.exception("Error processing message:")
        message.nack()

def listen_for_messages():
    logger.info(f"Listening to Pub/Sub subscription: {subscription_path}")
    while not shutdown_event.is_set():
        try:
            future = subscriber.subscribe(subscription_path, callback=callback)
            logger.info("PTO Update worker is now actively listening for messages...")
            future.result()
        except Exception as e:
            logger.exception("Pub/Sub listener crashed. Restarting in 5 seconds...")
            time.sleep(5)

def run():
    if threading.current_thread() == threading.main_thread():
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Starting PTO Update microservice...")

    def heartbeat():
        while not shutdown_event.is_set():
            logger.info("Heartbeat: PTO Update microservice is alive")
            time.sleep(300)

    threading.Thread(target=heartbeat, daemon=True).start()

    try:
        listen_for_messages()
    except Exception as e:
        logger.exception("Unhandled exception in run()")
    finally:
        try:
            subscriber.close()
            logger.info("Subscriber client closed.")
        except Exception as e:
            logger.warning("Failed to close subscriber cleanly: %s", e)

        logger.info("PTO Update microservice shut down.")
