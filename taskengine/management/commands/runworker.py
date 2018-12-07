__author__ = 'Dmitry Golubkov'

from django.core.management.base import BaseCommand
from taskengine.taskdef import TaskDefinition
from taskengine.metadata import AMIClient
from deftcore.log import Logger

logger = Logger.get()


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '-n',
            '--name',
            dest='worker_name',
            choices=['process_requests',
                     'sync_ami_projects',
                     'sync_ami_types',
                     'sync_ami_phys_containers',
                     'sync_ami_tags'],
            help=''
        )

        parser.add_argument(
            '-t',
            '--types',
            type=str,
            dest='request_types',
            help=''
        )

    def handle(self, *args, **options):
        if options['worker_name'] == 'process_requests':
            request_types = None
            if 'request_types' in options.keys():
                if options['request_types']:
                    request_types = options['request_types'].split(',')
            engine = TaskDefinition()
            engine.process_requests(restart=False, no_wait=False, request_types=request_types)
        elif options['worker_name'] == 'sync_ami_projects':
            client = AMIClient()
            client.sync_ami_projects()
        elif options['worker_name'] == 'sync_ami_types':
            client = AMIClient()
            client.sync_ami_types()
        elif options['worker_name'] == 'sync_ami_phys_containers':
            client = AMIClient()
            client.sync_ami_phys_containers()
        elif options['worker_name'] == 'sync_ami_tags':
            client = AMIClient()
            client.sync_ami_tags()
