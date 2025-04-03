import json
import time
import signal
from google.cloud import pubsub_v1
from pto_update.models import PTO
from utils.dashboard_events import build_dashboard_payload

# Initialize Pub/Sub clients
subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

class PTOProcessor:
    def __init__(self, project_id, subscription_name, topic_name):
        self.project_id = project_id
        self.subscription_name = subscription_name
        self.topic_name = topic_name

    def callback(self, message):
        """Callback function to process incoming messages."""
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
        publisher.publish(self.topic_name, json.dumps(dashboard_payload).encode("utf-8"))
        message.ack()

    def listen_for_messages(self):
        """Subscribe to the given Pub/Sub subscription and process messages."""
        # Subscription path
        subscription_path = subscriber.subscription_path(self.project_id, self.subscription_name)

        # Callback function to process each received message
        subscriber.subscribe(subscription_path, callback=self.callback)

        # Keep the subscriber running
        print("Listening for messages...")

        while True:
            time.sleep(1)  # Avoid busy-waiting and keep the process running

    def run(self):
        """Start the listener."""
        self.listen_for_messages()


# Graceful shutdown handling
def signal_handler(sig, frame):
    print('Shutting down gracefully...')
    exit(0)

# Register the signal handler for SIGINT (Ctrl+C)
signal.signal(signal.SIGINT, signal_handler)

# Create the processor and run it
processor = PTOProcessor(
    project_id='your-project-id',
    subscription_name='pto-deduction-subscription',
    topic_name='projects/your-project-id/topics/dashboard-topic'
)
processor.run()

