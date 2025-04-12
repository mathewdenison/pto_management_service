from django.core.management import BaseCommand
from threading import Thread, Event
from importlib import import_module
import logging
import time
import uvicorn

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

        threads = {}
        def launch_service(service_name):
            logger.info(f"Launching service: {service_name}")
            module = import_module(service_name)
            thread = Thread(target=module.run, name=service_name)
            thread.start()
            threads[service_name] = thread
            logger.info(f"Started {service_name}")

        # Start all services
        for service in services:
            launch_service(service)

        # Start a watchdog thread that checks for dead threads
        def watchdog():
            while True:
                for name, thread in list(threads.items()):
                    if not thread.is_alive():
                        logger.warning(f"Service thread {name} died. Restarting...")
                        launch_service(name)
                time.sleep(10)

        watchdog_thread = Thread(target=watchdog, name="watchdog")
        watchdog_thread.start()

        # Start health check server in a separate thread (so it doesn’t block)
        def start_health_server():
            logger.info("Starting health check service on port 8080")
            uvicorn.run("health:app", host="0.0.0.0", port=8080)

        health_thread = Thread(target=start_health_server, name="health-server")
        health_thread.start()

        # Block the main thread so daemon threads don’t die
        watchdog_thread.join()
