from django.core.management import BaseCommand
from multiprocessing import Process
from importlib import import_module


class Command(BaseCommand):
    help = 'Run all services'

    def handle(self, *args, **options):
        # List of services
        services = [
            'pto_update.scripts.process_messages',
            'pto_usage.scripts.process_messages',
        ]

        # Start all services
        processes = []
        for service in services:
            # Import the script module
            module = import_module(service)

            # Start the service in a new process
            process = Process(target=module.run)
            process.start()
            processes.append(process)

        # Wait for all services to complete
        for process in processes:
            process.join()
