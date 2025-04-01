import json
import boto3
from pto_update.models import PTO
from utils.dashboard_events import build_dashboard_payload

# Initialize SQS
sqs = boto3.client('sqs')
pto_deduction_queue_url = sqs.get_queue_url(QueueName='pto_deduction_queue')['QueueUrl']
dashboard_queue_url = sqs.get_queue_url(QueueName='dashboard_queue')['QueueUrl']

# Listener
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
            data = json.loads(message['Body'])
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

            sqs.send_message(QueueUrl=dashboard_queue_url, MessageBody=json.dumps(dashboard_payload))
            sqs.delete_message(QueueUrl=pto_deduction_queue_url, ReceiptHandle=message['ReceiptHandle'])
