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
logger = logging.getLogger("user_pto_lookup")
logger.setLevel(logging.INFO)

# GCP configuration
project_id = "hopkinstimesheetproj"
subscription_name = "user_pto_queue-sub"
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
        logger.info("Received PTO lookup message.")
        raw_data = message.data.decode("utf-8")
        logger.info(f"Raw message received: {raw_data}")

        # First decode
        data = json.loads(raw_data)
        # If update_data is a string (i.e. still JSON-encoded), decode it again.
        if isinstance(data, str):
            data = json.loads(data)
        employee_id = data["employee_id"]
        logger.info(f"Looking up PTO for employee_id: {employee_id}")

        # Safety check: Get or create the PTO object
        # Try to get existing PTO object
        pto = PTO.get_by_employee_id(employee_id)
        created = False

        # If not found, create a new one
        if not pto:
            pto = PTO(employee_id=employee_id, balance=0)
            pto.save()
            created = True

        if created:
            msg = f"Created new PTO record for employee_id {employee_id} with 0 balance."
            logger.info(msg)
        else:
            msg = f"PTO balance for employee_id {employee_id} is {pto.balance} hours."
            logger.info(msg)

        payload = build_dashboard_payload(
            employee_id,
            "pto_lookup",
            msg,
            {"pto_balance": pto.balance}
        )

        publisher.publish(dashboard_topic, json.dumps(payload).encode("utf-8"))
        logger.info("Published PTO balance update to dashboard topic.")
        message.ack()
        logger.info("Message acknowledged.")

    except Exception as e:
        logger.exception(f"Error processing PTO lookup message: {str(e)}")
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
