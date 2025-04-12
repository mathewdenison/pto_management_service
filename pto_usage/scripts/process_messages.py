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
logger = logging.getLogger("pto_deduction_handler")
logger.setLevel(logging.INFO)

# GCP Pub/Sub configuration
project_id = "hopkinstimesheetproj"
subscription_name = "pto_deduction_sub"
dashboard_topic = f"projects/{project_id}/topics/dashboard-queue"
subscription_path = pubsub_v1.SubscriberClient().subscription_path(project_id, subscription_name)

subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

# Graceful shutdown event
shutdown_event = threading.Event()

def signal_handler(sig, frame):
    logger.info("Received termination signal. Preparing for shutdown...")
    shutdown_event.set()

def callback(message):
    try:
        logger.info("Received PTO deduction message.")
        raw_data = message.data.decode("utf-8")
        logger.info(f"Raw message received: {raw_data}")

        data = json.loads(raw_data)
        if isinstance(data, str):
            data = json.loads(data)
        logger.info(f"Payload: {data}")

        employee_id = data["employee_id"]
        pto_hours = data["pto_hours"]

        pto = PTO.get_by_employee_id(employee_id)
        created = False

        if not pto:
            pto = PTO(employee_id=employee_id, balance=0)
            pto.save()
            created = True

        if created:
            logger.info(f"Created new PTO record for employee_id {employee_id} with 0 balance.")

        if pto.balance < pto_hours:
            msg = (
                f"Not enough PTO balance for employee_id {employee_id}. "
                f"Current: {pto.balance}, Requested: {pto_hours}"
            )
            logger.warning(msg)
            dashboard_payload = build_dashboard_payload(employee_id, "pto_deducted", msg)
        else:
            pto.balance -= pto_hours
            pto.save()
            msg = (
                f"PTO successfully deducted for employee_id {employee_id}. "
                f"New balance: {pto.balance}"
            )
            logger.info(msg)
            dashboard_payload = build_dashboard_payload(
                employee_id,
                "refresh_data",
                "Time log created, please refresh dashboard data.",
            )

        publisher.publish(dashboard_topic, json.dumps(dashboard_payload).encode("utf-8"))
        logger.info("Published dashboard update.")
        message.ack()
        logger.info("Message acknowledged.")

    except Exception as e:
        logger.exception("Failed to handle message:")
        message.nack()

def listen_for_messages():
    logger.info(f"Listening to Pub/Sub subscription: {subscription_path}")

    while not shutdown_event.is_set():
        try:
            future = subscriber.subscribe(subscription_path, callback=callback)
            logger.info("PTO Deduction handler is actively listening for messages...")
            future.result()  # blocks until failure
        except Exception as e:
            logger.exception("Pub/Sub listener crashed. Restarting in 5 seconds...")
            time.sleep(5)

def run():
    if threading.current_thread() == threading.main_thread():
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Starting PTO Deduction microservice...")

    def heartbeat():
        while not shutdown_event.is_set():
            logger.info("Heartbeat: PTO Deduction microservice is alive")
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

        logger.info("PTO Deduction microservice shut down.")
