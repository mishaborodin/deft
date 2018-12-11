__author__ = 'Dmitry Golubkov'

from django.core.management.base import BaseCommand
from taskengine.taskdef import TaskDefinition
from taskengine.metadata import AMIClient
from taskengine.models import ProductionDataset
from taskengine.rucioclient import RucioClient
from django.utils import timezone
import logging

logger = logging.getLogger('deftcore.worker')


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
                     'sync_ami_tags',
                     'check_datasets'],
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
        elif options['worker_name'] == 'check_datasets':
            client = RucioClient()
            for dataset in ProductionDataset.objects.filter(ddm_status=None, ddm_timestamp=None).iterator():
                if not client.is_dsn_exist(dataset.name):
                    dataset.ddm_timestamp = timezone.now()
                    dataset.ddm_status = 'erase'
                    dataset.save()
                    logger.info('updated dataset {0} with ddm_status="{1}" and ddm_timestamp="{2}"'.format(
                        dataset.name,
                        dataset.ddm_status,
                        dataset.ddm_timestamp)
                    )
