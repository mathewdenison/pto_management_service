def build_dashboard_payload(employee_id, event_type, message, extra_payload=None):
    return {
        "employee_id": employee_id,
        "type": event_type,
        "payload": {
            "message": message,
            **(extra_payload or {})
        }
    }
