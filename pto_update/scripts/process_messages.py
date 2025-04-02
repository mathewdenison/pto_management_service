import json
from google.cloud import pubsub_v1
from utils.pto_update_manager import PTOUpdateManager
from utils.dashboard_events import build_dashboard_payload

# Initialize Pub/Sub clients
subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

# GCP Pub/Sub subscription and topic names
pto_update_subscription = 'projects/your-project-id/subscriptions/pto-update-processing-subscription'
dashboard_topic = 'projects/your-project-id/topics/dashboard-topic'

# Listener function to handle incoming messages
def callback(message):
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
    publisher.publish(dashboard_topic, json.dumps(dashboard_payload).encode("utf-8"))
    message.ack()

# Subscribe to the PTO update queue and listen for messages
subscriber.subscribe(pto_update_subscription, callback=callback)

# Keep the subscriber running indefinitely
while True:
    pass
