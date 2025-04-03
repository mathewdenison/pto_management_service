import json
import time
import signal
from google.cloud import pubsub_v1
from pto_update.models import PTO
from utils.dashboard_events import build_dashboard_payload

# GCP Pub/Sub subscription and topic names
project_id = "your-project-id"
subscription_name = "pto-deduction-subscription"
dashboard_topic = "projects/your-project-id/topics/dashboard-topic"

# Initialize Pub/Sub clients
subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

# Subscription path
subscription_path = subscriber.subscription_path(project_id, subscription_name)

# Graceful shutdown handler
def signal_handler(sig, frame):
    """Graceful shutdown handler."""
    print('Shutting down gracefully...')
    exit(0)

# Callback function to process incoming messages
def callback(message):
    data = json.loads(message.data.decode("utf-8"))
    employee_id = data['employee_id']
    pto_hours = data['pto_hours']

    try:
        pto = PTO.objects.get(employee_id=employee_id)

        if pto.balance < pto_hours:
            msg = f"Not enough PTO balance for employee_id {employee_id}."
            print(f"[DEDUCT FAIL] {msg}")
            dashboard_payload = build_dashboard_payload(employee_id, "pto_deducted", msg)
        else:
            pto.balance -= pto_hours
            pto.save()

            msg = f"PTO for employee_id {employee_id} successfully decreased."
            print(f"[DEDUCT SUCCESS] {msg}")
            dashboard_payload = build_dashboard_payload(
                employee_id,
                "pto_deducted",
                msg,
                {"new_pto_balance": pto.balance}
            )

    except PTO.DoesNotExist:
        msg = f"PTO for employee_id {employee_id} does not exist."
        print(f"[DEDUCT FAIL] {msg}")
        dashboard_payload = build_dashboard_payload(employee_id, "pto_deducted", msg)

    except Exception as e:
        msg = f"Error processing PTO deduction for employee_id {employee_id}: {str(e)}"
        print(f"[DEDUCT ERROR] {msg}")
        dashboard_payload = build_dashboard_payload(employee_id, "pto_deducted", msg)

    # Publish the message to the dashboard topic
    publisher.publish(dashboard_topic, json.dumps(dashboard_payload).encode("utf-8"))
    message.ack()

# Run the subscriber to listen for messages
def run():
    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)

    # Subscribe to the PTO deduction queue and listen for messages
    print("Listening for messages...")
    subscriber.subscribe(subscription_path, callback=callback)

    # Keep the subscriber running indefinitely with time.sleep to avoid busy-waiting
    while True:
        time.sleep(1)  # Avoid busy-waiting and keep the process running

if __name__ == "__main__":
    run()

