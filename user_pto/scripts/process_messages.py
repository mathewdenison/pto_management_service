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

        data = json.loads(raw_data)
        if isinstance(data, str):
            data = json.loads(data)

        employee_id = data["employee_id"]
        logger.info(f"Looking up PTO for employee_id: {employee_id}")

        # Retrieve or create PTO object
        pto = PTO.get_by_employee_id(employee_id)
        created = False

        if not pto:
            pto = PTO(employee_id=employee_id, balance=0)
            pto.save()
            created = True

        msg = (
            f"Created new PTO record for employee_id {employee_id} with 0 balance."
            if created else
            f"PTO balance for employee_id {employee_id} is {pto.balance} hours."
        )
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
        logger.exception("Error processing PTO lookup message:")
        message.nack()

def listen_for_messages():
    logger.info(f"Listening to Pub/Sub subscription: {subscription_path}")

    while not shutdown_event.is_set():
        try:
            future = subscriber.subscribe(subscription_path, callback=callback)
            logger.info("User PTO Lookup microservice is actively listening for messages...")
            future.result()  # blocks until failure
        except Exception as e:
            logger.exception("Pub/Sub listener crashed. Restarting in 5 seconds...")
            time.sleep(5)

def run():
    if threading.current_thread() == threading.main_thread():
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Starting User PTO Lookup microservice...")

    def heartbeat():
        while not shutdown_event.is_set():
            logger.info("Heartbeat: User PTO Lookup microservice is alive")
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

        logger.info("User PTO Lookup microservice shut down.")
