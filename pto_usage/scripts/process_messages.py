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

# Pub/Sub clients
subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

# Graceful shutdown control
shutdown_event = threading.Event()

def signal_handler(sig, frame):
    logger.info("Received termination signal. Preparing for shutdown...")
    shutdown_event.set()

def callback(message):
    try:
        logger.info("Received PTO deduction message.")
        data = json.loads(message.data.decode("utf-8"))
        logger.info(f"Payload: {data}")

        employee_id = data["employee_id"]
        pto_hours = data["pto_hours"]

        try:
            pto = PTO.objects.get(employee_id=employee_id)
            if pto.balance < pto_hours:
                msg = f"Not enough PTO balance for employee_id {employee_id}. Current: {pto.balance}, Requested: {pto_hours}"
                logger.warning(msg)
                dashboard_payload = build_dashboard_payload(employee_id, "pto_deducted", msg)
            else:
                pto.balance -= pto_hours
                pto.save()

                msg = f"PTO successfully deducted for employee_id {employee_id}. New balance: {pto.balance}"
                logger.info(msg)
                dashboard_payload = build_dashboard_payload(
                    employee_id,
                    "pto_deducted",
                    msg,
                    {"new_pto_balance": pto.balance}
                )

        except PTO.DoesNotExist:
            msg = f"PTO record not found for employee_id {employee_id}"
            logger.warning(msg)
            dashboard_payload = build_dashboard_payload(employee_id, "pto_deducted", msg)

        except Exception as e:
            msg = f"Unexpected error for employee_id {employee_id}: {str(e)}"
            logger.exception(msg)
            dashboard_payload = build_dashboard_payload(employee_id, "pto_deducted", msg)

        publisher.publish(dashboard_topic, json.dumps(dashboard_payload).encode("utf-8"))
        logger.info("Published dashboard update.")
        message.ack()
        logger.info("Message acknowledged.")

    except Exception as e:
        logger.exception(f"Failed to handle message: {str(e)}")
        message.nack()

def run():
    if threading.current_thread() == threading.main_thread():
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    logger.info(f"Subscribing to {subscription_path}")
    future = subscriber.subscribe(subscription_path, callback=callback)
    logger.info("PTO deduction service is now running...")

    try:
        while not shutdown_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Shutting down...")
        shutdown_event.set()
    finally:
        future.cancel()
        subscriber.close()
        logger.info("Subscriber cancelled and client closed. Service terminated.")
