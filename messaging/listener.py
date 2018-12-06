__author__ = 'Dmitry Golubkov'

import stomp
import json
import re
from taskengine.models import TProject, ProductionDataset
from django.utils import timezone


class Listener(stomp.ConnectionListener):
    def __init__(self, logger, no_db_log=False):
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
        pass

    def on_message(self, headers, message_s):
        message = json.loads(message_s)

        event_type = message['event_type'].lower()
        payload = message['payload']

        scope = payload.get('scope', None)
        name = payload.get('name', None)
        account = payload.get('account', None)

        if not scope in self._scopes:
            return

        # ERASE - erase dataset
        # ERASE_CNT - erase container
        # CREATE_DTS - create dataset
        # CREATE_CNT - create container
        # deletion-done - delete file
        # LOST - lost file

        if event_type in ('ERASE'.lower()):
            if self.is_dataset_ignored(name):
                return
            self._logger.info(
                '[DELETION ({0})]: scope={1}, name={2}, account={3}'.format(event_type, scope, name, account)
            )
            if not self._no_db_log:
                dataset = ProductionDataset.objects.get(name=name.split(':')[-1])
                if dataset:
                    dataset.ddm_timestamp = timezone.now()
                    dataset.ddm_status = 'erase'
                    dataset.save()
