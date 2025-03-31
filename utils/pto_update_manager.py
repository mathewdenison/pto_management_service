from pto_update.models import PTO


class PTOUpdateManager:
    def __init__(self, employee_id, new_balance):
        self.employee_id = employee_id
        self.new_balance = new_balance

    def update_pto(self):
        try:
            # Get the PTO object
            pto = PTO.objects.get(employee__id=self.employee_id)

            # Update the PTO balance
            pto.balance = self.new_balance
            pto.save()

            return {"result": "success"}
        except PTO.DoesNotExist:
            return {"result": "failure", "message": f"PTO for employee_id:{self.employee_id} does not exist."}
        except Exception as e:
            return {"result": "failure", "message": str(e)}
