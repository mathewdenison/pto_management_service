from django.core.management import BaseCommand
from threading import Thread, Event
from importlib import import_module
from uvicorn import run as run_uvicorn
import logging
import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class Command(BaseCommand):
    help = 'Run all PTO-related services'

    def handle(self, *args, **options):
        services = [
            'bulk_pto.scripts.process_messages',
            'pto_deduction.scripts.process_messages',
            'pto_update.scripts.process_messages',
            'pto_usage.scripts.process_messages',
            'user_pto.scripts.process_messages',
        ]

        thread_refs = {}  # Keep track of threads and their targets
        shutdown_event = Event()

        def start_thread(service_path):
            logger.info(f"Launching service: {service_path}")
            module = import_module(service_path)
            thread = Thread(target=module.run, daemon=True)
            thread.start()
            logger.info(f"Started service thread: {service_path}")
            return thread

        # Start all threads initially
        for service in services:
            thread_refs[service] = start_thread(service)

        # Watchdog to restart threads if they crash
        def watchdog():
            while not shutdown_event.is_set():
                for service, thread in list(thread_refs.items()):
                    if not thread.is_alive():
                        logger.warning(f"Service {service} thread died. Restarting...")
                        thread_refs[service] = start_thread(service)
                time.sleep(10)  # Adjust watchdog interval as needed

        Thread(target=watchdog, daemon=True).start()

        # Start the health check FastAPI app to block and keep Cloud Run alive
        logger.info("Starting health check FastAPI server on port 8080")
        run_uvicorn("health:app", host="0.0.0.0", port=8080)
