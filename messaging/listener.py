__author__ = 'Dmitry Golubkov'

import stomp
import json
import re
from taskengine.models import TProject, ProductionDataset
from taskengine.protocol import TaskDefConstants
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

        if not scope in self._scopes:
            return

        if event_type in (TaskDefConstants.DDM_ERASE_EVENT_TYPE.lower()):
            if self.is_dataset_ignored(name):
                return
            self._logger.info(
                '[DELETION ({0})]: scope={1}, name={2}, account={3}'.format(event_type, scope, name, account)
            )
            if not self._no_db_log:
                dataset = ProductionDataset.objects.get(name=name.split(':')[-1])
                if dataset:
                    dataset.ddm_timestamp = timezone.now()
                    dataset.ddm_status = TaskDefConstants.DDM_ERASE_STATUS
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
                dataset = ProductionDataset.objects.get(name=dataset_name.split(':')[-1])
                if dataset:
                    dataset.ddm_timestamp = timezone.now()
                    dataset.ddm_status = TaskDefConstants.DDM_LOST_STATUS
                    dataset.save()
        elif event_type in (TaskDefConstants.DDM_PROGRESS_EVENT_TYPE.lower()):
            rule_id = payload.get('rule_id', None)
            progress = payload.get('progress', None)
            # if self.is_dataset_ignored(name):
            #     return
            self._logger.info(
                '[PROGRESS ({0})]: scope={1}, name={2}, rule_id={3}, progress={4}'.format(
                    event_type,
                    scope,
                    name,
                    rule_id,
                    progress
                )
            )
