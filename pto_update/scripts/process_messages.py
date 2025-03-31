import json
import boto3

from utils.pto_update_manager import PTOUpdateManager

# Initialize sqs
sqs = boto3.client('sqs')

# Get URLs for SQS queues
pto_update_queue_url = sqs.get_queue_url(QueueName='pto_update_processing_queue')['QueueUrl']
dashboard_queue_url = sqs.get_queue_url(QueueName='dashboard_queue')['QueueUrl']

# Continuously listen for messages
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
            update_manager = PTOUpdateManager(update_data['employee_id'], update_data['new_balance'])

            result = update_manager.update_pto()
            dashboard_data = {"employee_id": update_data['employee_id']}

            if result['result'] == "success":
                print(f"PTO for employee_id {update_data['employee_id']} successfully updated.")

                # Add success specific data
                dashboard_data["new_pto_balance"] = update_data['new_balance']
                dashboard_data["message"] = f"PTO for employee_id {update_data['employee_id']} successfully updated."

            else:
                print(f"Failed to update PTO for employee_id {update_data['employee_id']}. Reason: {result['message']}")

                # Add failure specific data
                dashboard_data[
                    "message"] = f"Failed to update PTO for employee_id {update_data['employee_id']}. Reason: {result['message']}"

            # Send status to the dashboard queue regardless of result
            sqs.send_message(QueueUrl=dashboard_queue_url, MessageBody=json.dumps(dashboard_data))
            print(f"PTO update status for employee_id {update_data['employee_id']} sent to the dashboard queue.")

            sqs.delete_message(QueueUrl=pto_update_queue_url, ReceiptHandle=message['ReceiptHandle'])
