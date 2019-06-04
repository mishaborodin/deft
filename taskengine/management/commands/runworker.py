__author__ = 'Dmitry Golubkov'

import re
import os
from django.core.management.base import BaseCommand
from taskengine.taskdef import TaskDefinition
from taskengine.metadata import AMIClient
from taskengine.models import ProductionDataset
from taskengine.rucioclient import RucioClient
from django.utils import timezone
from django.db.models import Q
from django.core.exceptions import ObjectDoesNotExist
from taskengine.protocol import TaskDefConstants
from rucio.common.exception import CannotAuthenticate
from deftcore.security.voms import VOMSClient
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
                     'check_datasets',
                     'analyze_lost_files_report'],
            help=''
        )

        parser.add_argument(
            '-t',
            '--types',
            type=str,
            dest='request_types',
            help=''
        )

        parser.add_argument(
            '-e',
            '--extra',
            type=str,
            dest='extra_param',
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
            try:
                client = RucioClient()
                query = 'SELECT name, status, ddm_status, ddm_timestamp FROM t_production_dataset ' + \
                        'where status is not NULL ORDER BY taskid DESC'
                if options['extra_param']:
                    if options['extra_param'] == '1':
                        name = ''
                        query = "SELECT name, status, ddm_status, ddm_timestamp FROM t_production_dataset " + \
                                "WHERE name='{0}'".format(name)
                    elif options['extra_param'] == '2':
                        query = "SELECT name, status, ddm_status, ddm_timestamp FROM t_production_dataset " + \
                                "WHERE status is not NULL " + \
                                "AND timestamp > TO_DATE('01-10-2018', 'DD-MM-YYYY') " + \
                                "AND timestamp < TO_DATE('01-02-2019', 'DD-MM-YYYY') " + \
                                "ORDER BY timestamp ASC"
                        logger.info('check_datasets, pid={0}, query=\"{1}\"'.format(os.getpid(), query))
                for dataset in ProductionDataset.objects.raw(query):
                    current_timestamp = timezone.now()

                    try:
                        status = client.is_dsn_exist(dataset.name)
                    except CannotAuthenticate:
                        voms_client = VOMSClient()
                        voms_client.get()
                        if not voms_client.valid:
                            raise Exception('check_datasets, cannot initialize VOMS proxy')
                        status = client.is_dsn_exist(dataset.name)
                    except Exception as ex:
                        logger.warning('check_datasets, is_dsn_exist (%s) failed: %s', dataset.name, str(ex))
                        continue

                    if dataset.status == TaskDefConstants.DATASET_DELETED_STATUS:
                        if status:
                            dataset.timestamp = current_timestamp
                            dataset.status = TaskDefConstants.DATASET_TO_BE_DELETED_STATUS
                            dataset.ddm_timestamp = None
                            dataset.ddm_status = None
                            dataset.save()
                            logger.info('check_datasets, updated dataset STATUS (%s): %s (task_id=%d)',
                                        TaskDefConstants.DATASET_TO_BE_DELETED_STATUS, dataset.name, dataset.task_id)
                        else:
                            if not dataset.ddm_status or not dataset.ddm_timestamp:
                                dataset.ddm_timestamp = current_timestamp
                                dataset.ddm_status = TaskDefConstants.DDM_ERASE_STATUS
                                dataset.save()
                                logger.info('check_datasets, updated dataset DDM_* info: %s (task_id=%d)',
                                            dataset.name, dataset.task_id)
                    else:
                        if not status:
                            if (not dataset.ddm_status) or (not dataset.ddm_timestamp):
                                dataset.ddm_timestamp = current_timestamp
                                dataset.ddm_status = TaskDefConstants.DDM_ERASE_STATUS
                            dataset.status = TaskDefConstants.DATASET_DELETED_STATUS
                            dataset.save()
                            logger.info('check_datasets, updated dataset STATUS (%s): %s (task_id=%d)',
                                        TaskDefConstants.DATASET_DELETED_STATUS, dataset.name, dataset.task_id)
            except Exception as ex:
                logger.exception('check_datasets failed: {0}'.format(str(ex)))
        elif options['worker_name'] == 'analyze_lost_files_report':
            path = options['extra_param']
            report = None
            with open(path, 'r') as fp:
                report = fp.readlines()
            if report:
                for line in report:
                    result = re.match(r'^.+_tid(?P<tid>\d+)_00.+$', line)
                    if result:
                        dsn_name = line.split(' ')[3]
                        task_id = int(result.groupdict()['tid'])
                        try:
                            dataset = ProductionDataset.objects.get(
                                name=dsn_name,
                                task_id=task_id,
                                ddm_status=None,
                                ddm_timestamp=None
                            )
                            dataset.ddm_timestamp = timezone.now()
                            dataset.ddm_status = TaskDefConstants.DDM_LOST_STATUS
                            dataset.save()
                            logger.info(
                                'analyze_lost_files_report, updated dataset {0} with ddm_status="{1}" and ddm_timestamp="{2}"'.format(
                                    dataset.name,
                                    dataset.ddm_status,
                                    dataset.ddm_timestamp)
                            )
                        except ObjectDoesNotExist:
                            continue
