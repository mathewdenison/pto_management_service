import json
import time
import signal
import logging
from google.cloud import pubsub_v1
from pto_update.models import PTO
from utils.dashboard_events import build_dashboard_payload
from google.cloud import logging as cloud_logging

# Initialize Cloud Logging
cloud_client = cloud_logging.Client()
cloud_client.setup_logging()

# Setup standard logger
logger = logging.getLogger("pto_deduction_handler")
logger.setLevel(logging.INFO)

# GCP Pub/Sub config
project_id = "hopkinstimesheetproj"
subscription_name = "pto_deduction_sub"
dashboard_topic = "projects/hopkinstimesheetproj/topics/dashboard-queue"

# Pub/Sub clients
subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

def signal_handler(sig, frame):
    logger.info("Received termination signal. Shutting down...")
    exit(0)

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

        # Publish dashboard update
        publisher.publish(dashboard_topic, json.dumps(dashboard_payload).encode("utf-8"))
        logger.info("Published dashboard update.")
        message.ack()
        logger.info("Message acknowledged.")

    except Exception as e:
        logger.exception(f"Failed to handle message: {str(e)}")
        message.nack()

def run():
    signal.signal(signal.SIGINT, signal_handler)
    logger.info(f"Starting PTO deduction subscriber on subscription: {subscription_path}")
    subscriber.subscribe(subscription_path, callback=callback)
    logger.info("Listening for PTO deduction messages...")

    while True:
        time.sleep(60)
