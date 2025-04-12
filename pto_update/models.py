from google.cloud import firestore
from typing import Optional, List

db = firestore.Client()

class PTO:
    def __init__(self, employee_id: str, balance: int = 0):
        self.employee_id = str(employee_id)
        self.balance = balance

    def to_dict(self):
        return {
            "balance": self.balance
        }

    @staticmethod
    def from_dict(doc_id: str, data: dict):
        return PTO(
            employee_id=doc_id,
            balance=data.get("balance", 0)
        )

    def save(self):
        doc_ref = db.collection("pto").document(self.employee_id)
        doc_ref.set(self.to_dict())

    @staticmethod
    def get_by_employee_id(employee_id: str) -> Optional["PTO"]:
        doc = db.collection("pto").document(str(employee_id)).get()
        if doc.exists:
            return PTO.from_dict(doc.id, doc.to_dict())
        return None

    @staticmethod
    def all() -> List["PTO"]:
        docs = db.collection("pto").stream()
        return [PTO.from_dict(doc.id, doc.to_dict()) for doc in docs]

    def delete(self):
        db.collection("pto").document(self.employee_id).delete()

    def __str__(self):
        return f"PTO balance for employee {self.employee_id}: {self.balance} hours"
