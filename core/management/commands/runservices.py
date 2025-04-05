import logging
from django.core.management import BaseCommand
from multiprocessing import Process
from importlib import import_module

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class Command(BaseCommand):
    help = 'Run all services'

    def handle(self, *args, **options):
        services = [
            'pto_update.scripts.process_messages',
            'pto_usage.scripts.process_messages',
            'user_pto.scripts.process_messages',
        ]

        processes = []
        for service in services:
            logger.info(f"Launching service: {service}")
            module = import_module(service)
            process = Process(target=module.run)
            process.start()
            logger.info(f"Started {service} with PID {process.pid}")
            processes.append(process)

        for process in processes:
            process.join()

