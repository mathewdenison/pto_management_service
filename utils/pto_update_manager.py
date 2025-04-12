from pto_update.models import PTO

class PTOUpdateManager:
    def __init__(self, employee_id, new_balance=None):
        self.employee_id = employee_id
        self.new_balance = new_balance

    def get_current_balance(self):
        # Try to get existing PTO object
        pto = PTO.get_by_employee_id(self.employee_id)
        created = False

        # If not found, create a new one
        if not pto:
            pto = PTO(employee_id=self.employee_id, balance=0)
            pto.save()
            created = True
        return pto.balance

    def update_pto(self):
        pto = PTO.get_by_employee_id(self.employee_id)
        created = False

        # If not found, create a new one
        if not pto:
            pto = PTO(employee_id=self.employee_id, balance=0)
            pto.save()
            created = True

            return {
                "result": "success",
                "message": f"PTO balance updated to {self.new_balance}"
            }
        else:
            return {
                "result": "error",
                "message": "Invalid PTO object"
            }

    def subtract_pto(self, deduction):
        """
        Subtract the specified deduction from the current PTO balance.
        """
        try:
            # Ensure a PTO record exists, default balance is 0.
            pto = PTO.get_by_employee_id(self.employee_id)
            if not pto:
                pto = PTO(employee_id=self.employee_id, balance=0)
            # Update the PTO balance to new_balance.
            pto.balance = self.new_balance
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
