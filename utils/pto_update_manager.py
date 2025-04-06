from pto_update.models import PTO

class PTOUpdateManager:
    def __init__(self, employee_id, new_balance):
        self.employee_id = employee_id
        self.new_balance = new_balance

    def update_pto(self):
        try:
            # Ensures PTO record exists
            pto, created = PTO.objects.get_or_create(
                employee_id=self.employee_id,
                defaults={"balance": 0}
            )

            if created:
                msg = f"Created new PTO record for employee {self.employee_id} with 0 balance"

            # Update the PTO balance
            pto.balance = self.new_balance
            pto.save()

            return {
                "result": "success",
                "message": f"PTO balance updated to {self.new_balance}"
            }
        except Exception as e:
            return {
                "result": "error",
                "message": str(e)
            }
