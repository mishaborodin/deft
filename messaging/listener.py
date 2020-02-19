__author__ = 'Dmitry Golubkov'

import stomp
import json
import re
from taskengine.models import TProject, ProductionDataset, DatasetStaging
from taskengine.protocol import TaskDefConstants
from taskengine.rucioclient import RucioClient
from django.utils import timezone


class Listener(stomp.ConnectionListener):
    def __init__(self, client, logger, no_db_log=False):
        self._client = client
        self._logger = logger
        self._scopes = [e.project for e in TProject.objects.all()]
        self._no_db_log = no_db_log
        super(Listener, self).__init__()

    @staticmethod
    def is_dataset_ignored(name):
        is_sub_dataset = re.match(r'^.+_(sub|dis)\d+$', name)
        is_o10_dataset = re.match(r'^.+.o10$', name)
        return is_sub_dataset or is_o10_dataset

    def on_error(self, headers, message):
        self._logger.error('received an error: {0}'.format(message))

    def on_disconnected(self):
        self._logger.warning('disconnected')
        self._client.connect()

    def on_message(self, headers, message_s):
        message = json.loads(message_s)

        event_type = message['event_type'].lower()
        payload = message['payload']

        scope = payload.get('scope', None)
        name = payload.get('name', None)
        account = payload.get('account', None)

        if scope not in self._scopes:
            return

        if event_type in (TaskDefConstants.DDM_ERASE_EVENT_TYPE.lower()):
            if self.is_dataset_ignored(name):
                return
            self._logger.info(
                '[DELETION ({0})]: scope={1}, name={2}, account={3}'.format(event_type, scope, name, account)
            )
            if not self._no_db_log:
                dataset = ProductionDataset.objects.filter(name=name.split(':')[-1]).first()
                if dataset:
                    dataset.ddm_timestamp = timezone.now()
                    dataset.ddm_status = TaskDefConstants.DDM_ERASE_STATUS
                    dataset.status = TaskDefConstants.DATASET_DELETED_STATUS
                    dataset.timestamp = timezone.now()
                    dataset.save()
        elif event_type in (TaskDefConstants.DDM_LOST_EVENT_TYPE.lower()):
            dataset_name = payload.get('dataset_name', None)
            dataset_scope = payload.get('dataset_scope', None)
            if self.is_dataset_ignored(dataset_name):
                return
            self._logger.info(
                '[LOST ({0})]: scope={1}, name={2}, dataset={3}, account={4}'.format(
                    event_type, dataset_scope, name, dataset_name, account
                )
            )
            if not self._no_db_log:
                dataset = ProductionDataset.objects.filter(name=dataset_name.split(':')[-1]).first()
                if dataset:
                    dataset.ddm_timestamp = timezone.now()
                    dataset.ddm_status = TaskDefConstants.DDM_LOST_STATUS
                    dataset.save()
        elif event_type in (TaskDefConstants.DDM_PROGRESS_EVENT_TYPE.lower()):
            rule_id = payload.get('rule_id', None)
            progress = int(payload.get('progress', 0))
            current_timestamp = timezone.now()
            # dsn = name.split(':')[-1]
            dsn = name
            dataset_staging = DatasetStaging.objects.filter(dataset=dsn).first()
            if dataset_staging:
                last_progress = int(dataset_staging.staged_files * 100 / dataset_staging.total_files)
                if last_progress >= progress:
                    '[PROGRESS ({0})]: IGNORED, dsn={1}, progress={2}%, last_progress={3}%'.format(
                        event_type,
                        dsn,
                        progress,
                        last_progress
                    )
                    return

            self._logger.info(
                '[PROGRESS ({0})]: dsn={1}, rule_id={2}, progress={3}%'.format(
                    event_type,
                    dsn,
                    rule_id,
                    progress
                )
            )

            if not self._no_db_log:
                if dataset_staging:
                    dataset_staging.update_time = current_timestamp
                    if progress == 100:
                        dataset_staging.end_time = current_timestamp
                    dataset_staging.status = TaskDefConstants.DDM_STAGING_STATUS
                    dataset_staging.staged_files = int(progress * dataset_staging.total_files / 100)

                    try:
                        dataset_staging.save()
                    except Exception as ex:
                        self._logger.exception('Database problem: {0} ({1})'.format(str(ex), dsn))
                else:
                    total_files = 0

                    try:
                        rucio_client = RucioClient()
                        total_files = rucio_client.get_number_files(dsn)
                    except Exception as ex:
                        self._logger.exception(
                            'Rucio related problem detected (during getting total_files of dsn): {0}'.format(str(ex))
                        )

                    dataset_staging = DatasetStaging(dataset=dsn,
                                                     status=TaskDefConstants.DDM_STAGING_STATUS,
                                                     start_time=current_timestamp,
                                                     update_time=current_timestamp,
                                                     rse=rule_id,
                                                     total_files=total_files,
                                                     staged_files=int(progress * total_files / 100))
                    if progress == 100:
                        dataset_staging.end_time = current_timestamp

                    try:
                        dataset_staging.save()
                    except Exception as ex:
                        self._logger.exception('Database problem: {0} ({1})'.format(str(ex), dsn))
