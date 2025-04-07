from django.core.management import BaseCommand
from threading import Thread
from importlib import import_module
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class Command(BaseCommand):
    help = 'Run all services'

    def handle(self, *args, **options):
        services = [
            'bulk_pto.scripts.process_messages',
            'pto_deduction.scripts.process_messages',
            'pto_update.scripts.process_messages',
            'pto_usage.scripts.process_messages',
            'user_pto.scripts.process_messages',
        ]

        threads = []
        for service in services:
            logger.info(f"Launching service: {service}")
            module = import_module(service)
            thread = Thread(target=module.run, daemon=True)
            thread.start()
            logger.info(f"Started {service}")
            threads.append(thread)

        for thread in threads:
            thread.join()
