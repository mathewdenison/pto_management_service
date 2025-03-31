import json
import boto3

from pto_update.models import PTO

# Initialize sqs
sqs = boto3.client('sqs')

# Get URLs for SQS queues
pto_deduction_queue_url = sqs.get_queue_url(QueueName='pto_deduction_queue')['QueueUrl']
dashboard_queue_url = sqs.get_queue_url(QueueName='dashboard_queue')['QueueUrl']

# Continuously listen for messages
while True:
    messages = sqs.receive_message(
        QueueUrl=pto_deduction_queue_url,
        AttributeNames=['All'],
        MaxNumberOfMessages=1,
        MessageAttributeNames=['All'],
        VisibilityTimeout=30,
        WaitTimeSeconds=20
    )

    if 'Messages' in messages:
        for message in messages['Messages']:
            deduction_data = json.loads(message['Body'])

            # Prepare initial dashboard data
            dashboard_data = {"employee_id": deduction_data['employee_id']}

            try:
                pto = PTO.objects.get(employee_id=deduction_data['employee_id'])
                old_balance = pto.balance

                # Ensure enough balance for deduction
                if old_balance < deduction_data['pto_hours']:
                    print(f"Not enough PTO balance for employee_id {deduction_data['employee_id']}.")
                    dashboard_data["message"] = f"Not enough PTO balance for employee_id {deduction_data['employee_id']}."
                else:
                    pto.balance -= deduction_data['pto_hours']
                    pto.save()

                    print(f"PTO for employee_id {deduction_data['employee_id']} successfully decreased.")

                    # Add success specific data
                    dashboard_data["new_pto_balance"] = pto.balance
                    dashboard_data[
                        "message"] = f"PTO for employee_id {deduction_data['employee_id']} successfully decreased."

            except PTO.DoesNotExist:
                print(f"PTO for employee_id {deduction_data['employee_id']} does not exist.")
                dashboard_data["message"] = f"PTO for employee_id {deduction_data['employee_id']} does not exist."

            except Exception as e:
                print(f"Failed to decrease PTO for employee_id {deduction_data['employee_id']}. Reason: {str(e)}")
                dashboard_data[
                    "message"] = f"Failed to decrease PTO for employee_id {deduction_data['employee_id']}. Reason: {str(e)}"

            finally:
                # Send status to the dashboard queue regardless of result
                sqs.send_message(QueueUrl=dashboard_queue_url, MessageBody=json.dumps(dashboard_data))
                print(f"PTO deduction status for employee_id {deduction_data['employee_id']} sent to the dashboard queue.")

                sqs.delete_message(QueueUrl=pto_deduction_queue_url, ReceiptHandle=message['ReceiptHandle'])
