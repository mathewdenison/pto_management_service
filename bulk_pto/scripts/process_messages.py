import json
import time
import signal
import threading
import logging

from google.cloud import pubsub_v1
from pto_update.models import PTO
from utils.dashboard_events import build_dashboard_payload
from google.cloud import logging as cloud_logging

# Initialize Google Cloud Logging
cloud_client = cloud_logging.Client()
cloud_client.setup_logging()

# Standard logger setup
logger = logging.getLogger("bulk_pto_lookup")
logger.setLevel(logging.INFO)

# GCP configuration
project_id = "hopkinstimesheetproj"
subscription_name = "bulk_pto_queue-sub"
dashboard_topic = f"projects/{project_id}/topics/dashboard-queue"
subscription_path = pubsub_v1.SubscriberClient().subscription_path(project_id, subscription_name)

# Pub/Sub clients
subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

# Graceful shutdown event
shutdown_event = threading.Event()

def signal_handler(sig, frame):
    logger.info("Shutdown signal received. Exiting gracefully...")
    shutdown_event.set()

def callback(message):
    try:
        logger.info("Received bulk PTO lookup message.")
        data = json.loads(message.data.decode("utf-8"))
        logger.info(f"Bulk PTO lookup trigger payload: {data}")

        # Retrieve all PTO objects from the database.
        all_pto = PTO.all()
        pto_list = [{"employee_id": p.employee_id, "pto_balance": p.balance} for p in all_pto]
        msg_str = f"Bulk PTO lookup: found {len(pto_list)} records."
        logger.info(msg_str)

        # Build a dashboard payload containing all PTO records.
        payload = build_dashboard_payload(
            "all",                    # Use a special identifier (e.g., "all") for bulk messages.
            "bulk_pto_lookup",        # Type to indicate this is a bulk PTO lookup message.
            msg_str,
            {"pto_records": pto_list}
        )

        publisher.publish(dashboard_topic, json.dumps(payload).encode("utf-8"))
        logger.info("Published bulk PTO lookup update to dashboard topic.")
        message.ack()
        logger.info("Message acknowledged.")

    except Exception as e:
        logger.exception(f"Error processing bulk PTO lookup message: {str(e)}")
        message.nack()

def run():
    if threading.current_thread() == threading.main_thread():
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    logger.info(f"Subscribing to Pub/Sub: {subscription_path}")
    future = subscriber.subscribe(subscription_path, callback=callback)
    logger.info("User PTO lookup service is running and awaiting messages...")

    try:
        while not shutdown_event.is_set():
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt caught, preparing to shut down.")
        shutdown_event.set()
    finally:
        future.cancel()
        subscriber.close()
        logger.info("Subscriber cancelled and client closed. Service exited cleanly.")
