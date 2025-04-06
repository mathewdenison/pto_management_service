from pto_update.models import PTO

class PTOUpdateManager:
    def __init__(self, employee_id, new_balance=None):
        self.employee_id = employee_id
        self.new_balance = new_balance

    def get_current_balance(self):
        pto, created = PTO.objects.get_or_create(
            employee_id=self.employee_id,
            defaults={"balance": 0}
        )
        return pto.balance

    def update_pto(self):
        """
        Update the PTO balance to the value provided in self.new_balance.
        """
        try:
            # Ensure a PTO record exists, default balance is 0.
            pto, created = PTO.objects.get_or_create(
                employee_id=self.employee_id,
                defaults={"balance": 0}
            )
            # Update the PTO balance to new_balance.
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

    def subtract_pto(self, deduction):
        """
        Subtract the specified deduction from the current PTO balance.
        """
        try:
            # Ensure a PTO record exists, default balance is 0.
            pto, created = PTO.objects.get_or_create(
                employee_id=self.employee_id,
                defaults={"balance": 0}
            )
            # Calculate the new balance by subtracting the deduction.
            new_balance = pto.balance - deduction
            pto.balance = new_balance
            pto.save()

            return {
                "result": "success",
                "message": f"PTO balance updated to {new_balance}"
            }
        except Exception as e:
            return {
                "result": "error",
                "message": str(e)
            }
