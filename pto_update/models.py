from django.db import models


class PTO(models.Model):
    employee_id = models.IntegerField()  # stores the id of the employee
    balance = models.PositiveIntegerField(default=0)

    def __str__(self):
        return f"PTO balance for employee with id {self.employee_id}: {self.balance} hours"
