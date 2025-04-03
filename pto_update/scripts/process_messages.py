import json
import time
import signal
from google.cloud import pubsub_v1
from utils.pto_update_manager import PTOUpdateManager
from utils.dashboard_events import build_dashboard_payload

class PTOUpdateListener:
    def __init__(self, project_id, subscription_name, dashboard_topic):
        self.project_id = project_id
        self.subscription_name = subscription_name
        self.dashboard_topic = dashboard_topic
        self.subscriber = pubsub_v1.SubscriberClient()
        self.publisher = pubsub_v1.PublisherClient()
        self.subscription_path = self.subscriber.subscription_path(self.project_id, self.subscription_name)

    def signal_handler(self, sig, frame):
        """Graceful shutdown handler."""
        print('Shutting down gracefully...')
        exit(0)

    def callback(self, message):
        """Callback function to process incoming messages."""
        update_data = json.loads(message.data.decode("utf-8"))
        employee_id = update_data['employee_id']
        new_balance = update_data['new_balance']

        update_manager = PTOUpdateManager(employee_id, new_balance)
        result = update_manager.update_pto()

        if result['result'] == "success":
            print(f"[UPDATE] PTO for employee_id {employee_id} updated to {new_balance}")
            dashboard_payload = build_dashboard_payload(
                employee_id,
                "pto_updated",
                f"PTO for employee_id {employee_id} updated successfully.",
                {"new_pto_balance": new_balance}
            )
        else:
            print(f"[UPDATE] Failed to update PTO for employee_id {employee_id}. Reason: {result['message']}")
            dashboard_payload = build_dashboard_payload(
                employee_id,
                "pto_updated",
                f"Failed to update PTO for employee_id {employee_id}. Reason: {result['message']}"
            )

        # Publish the message to the dashboard topic
        self.publisher.publish(self.dashboard_topic, json.dumps(dashboard_payload).encode("utf-8"))
        message.ack()

    def run(self):
        """Main function to start the subscriber and listen for messages."""
        # Register signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)

        # Subscribe to the PTO update queue and listen for messages
        print("Listening for messages...")
        self.subscriber.subscribe(self.subscription_path, callback=self.callback)

        # Keep the subscriber running indefinitely with time.sleep to avoid busy-waiting
        while True:
            time.sleep(1)  # Avoid busy-waiting and keep the process running

if __name__ == "__main__":
    # Set up the listener and run
    listener = PTOUpdateListener(
        project_id="your-project-id",
        subscription_name="pto-update-processing-subscription",
        dashboard_topic="projects/your-project-id/topics/dashboard-topic"
    )
    listener.run()

