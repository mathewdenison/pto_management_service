import json
import boto3
from utils.pto_update_manager import PTOUpdateManager
from utils.dashboard_events import build_dashboard_payload

# Initialize SQS
sqs = boto3.client('sqs')
pto_update_queue_url = sqs.get_queue_url(QueueName='pto_update_processing_queue')['QueueUrl']
dashboard_queue_url = sqs.get_queue_url(QueueName='dashboard_queue')['QueueUrl']

# Listener
while True:
    messages = sqs.receive_message(
        QueueUrl=pto_update_queue_url,
        AttributeNames=['All'],
        MaxNumberOfMessages=1,
        MessageAttributeNames=['All'],
        VisibilityTimeout=30,
        WaitTimeSeconds=20
    )

    if 'Messages' in messages:
        for message in messages['Messages']:
            update_data = json.loads(message['Body'])
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

            sqs.send_message(QueueUrl=dashboard_queue_url, MessageBody=json.dumps(dashboard_payload))
            sqs.delete_message(QueueUrl=pto_update_queue_url, ReceiptHandle=message['ReceiptHandle'])
