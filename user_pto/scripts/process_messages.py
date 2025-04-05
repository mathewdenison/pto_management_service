import json
import time
import signal
import logging
from google.cloud import pubsub_v1
from pto_update.models import PTO
from utils.dashboard_events import build_dashboard_payload
from google.cloud import logging as cloud_logging

# Initialize Google Cloud logging
cloud_client = cloud_logging.Client()
cloud_client.setup_logging()

logger = logging.getLogger("user_pto_lookup")
logger.setLevel(logging.INFO)

# GCP configuration
project_id = "hopkinstimesheetproj"
subscription_name = "user_pto_sub"
dashboard_topic = f"projects/{project_id}/topics/dashboard-queue"
subscription_path = pubsub_v1.SubscriberClient().subscription_path(project_id, subscription_name)

subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

def signal_handler(sig, frame):
    logger.info("Shutdown signal received.")
    exit(0)

def callback(message):
    try:
        logger.info("Received PTO lookup message.")
        data = json.loads(message.data.decode("utf-8"))
        employee_id = data["employee_id"]
        logger.info(f"Looking up PTO for employee_id: {employee_id}")

        try:
            pto = PTO.objects.get(employee_id=employee_id)
            msg = f"PTO balance for employee_id {employee_id} is {pto.balance} hours."
            logger.info(msg)
            payload = build_dashboard_payload(
                employee_id,
                "pto_lookup",
                msg,
                {"pto_balance": pto.balance}
            )
        except PTO.DoesNotExist:
            msg = f"PTO record not found for employee_id {employee_id}."
            logger.warning(msg)
            payload = build_dashboard_payload(employee_id, "pto_lookup", msg, {"pto_balance": 0})

        publisher.publish(dashboard_topic, json.dumps(payload).encode("utf-8"))
        message.ack()
        logger.info("Published PTO balance and acknowledged message.")

    except Exception as e:
        logger.exception(f"Error processing PTO lookup message: {str(e)}")
        message.nack()

def run():
    signal.signal(signal.SIGINT, signal_handler)
    logger.info(f"Subscribing to {subscription_path}")
    subscriber.subscribe(subscription_path, callback=callback)
    logger.info("Waiting for PTO lookup messages...")
    while True:
        time.sleep(60)
