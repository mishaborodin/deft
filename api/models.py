__author__ = 'Dmitry Golubkov'

import json
from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils import timezone
from deftcore.log import Logger
from api import ApiServer
from deftcore.helpers import MetaProxy

logger = Logger().get()


class Request(models.Model):
    ACTION_TEST = 'test'
    ACTION_CLONE_TASK = 'clone_task'
    ACTION_ABORT_TASK = 'abort_task'
    ACTION_FINISH_TASK = 'finish_task'
    ACTION_REASSIGN_TASK = 'reassign_task'
    ACTION_CHANGE_TASK_PRIORITY = 'change_task_priority'
    ACTION_RETRY_TASK = 'retry_task'
    ACTION_CHANGE_TASK_RAM_COUNT = 'change_task_ram_count'
    ACTION_CHANGE_TASK_WALL_TIME = 'change_task_wall_time'
    ACTION_ADD_TASK_COMMENT = 'add_task_comment'
    ACTION_INCREASE_ATTEMPT_NUMBER = 'increase_attempt_number'
    ACTION_ABORT_UNFINISHED_JOBS = 'abort_unfinished_jobs'
    ACTION_CREATE_SLICE_TIER0 = 'create_slice_tier0'
    ACTION_OBSOLETE_TASK = 'obsolete_task'
    ACTION_CLEAN_TASK_CARRIAGES = 'clean_task_carriages'
    ACTION_KILL_JOB = 'kill_job'
    ACTION_SET_JOB_DEBUG_MODE = 'set_job_debug_mode'
    ACTION_CHANGE_TASK_CPU_TIME = 'change_task_cpu_time'
    ACTION_CHANGE_TASK_SPLIT_RULE = 'change_task_split_rule'
    ACTION_CHANGE_TASK_ATTRIBUTE = 'change_task_attribute'
    ACTION_PAUSE_TASK = 'pause_task'
    ACTION_RESUME_TASK = 'resume_task'
    ACTION_TRIGGER_TASK_BROKERAGE = 'trigger_task_brokerage'
    ACTION_AVALANCHE_TASK = 'avalanche_task'
    ACTION_REASSIGN_JOBS = 'reassign_jobs'
    ACTION_OBSOLETE_ENTITY = 'obsolete_entity'
    ACTION_SET_TTCR = 'set_ttcr'
    ACTION_SET_TTCJ = 'set_ttcj'
    ACTION_REASSIGN_TASK_TO_SHARE = 'reassign_task_to_share'
    ACTION_RELOAD_INPUT = 'reload_input'

    ACTION_LIST = (
        (ACTION_TEST, 'Test'),
        (ACTION_CLONE_TASK, 'Clone task'),
        (ACTION_ABORT_TASK, 'Abort task'),
        (ACTION_FINISH_TASK, 'Finish task'),
        (ACTION_REASSIGN_TASK, 'Reassign task to another cloud or site'),
        (ACTION_CHANGE_TASK_PRIORITY, 'Change task priority'),
        (ACTION_RETRY_TASK, 'Retry task'),
        (ACTION_CHANGE_TASK_RAM_COUNT, 'Change task RAM count'),
        (ACTION_CHANGE_TASK_WALL_TIME, 'Change task wall time'),
        (ACTION_ADD_TASK_COMMENT, 'Add task comment'),
        (ACTION_INCREASE_ATTEMPT_NUMBER, 'Increase attempt number'),
        (ACTION_ABORT_UNFINISHED_JOBS, 'Abort unfinished jobs in a task'),
        (ACTION_CREATE_SLICE_TIER0, 'Create slice in last Tier Zero request'),
        (ACTION_OBSOLETE_TASK, 'Obsolete task'),
        (ACTION_CLEAN_TASK_CARRIAGES, 'Delete task outputs with specified types'),
        (ACTION_KILL_JOB, 'Force kill single job with code=9'),
        (ACTION_SET_JOB_DEBUG_MODE, 'Setting debug mode for a job'),
        (ACTION_CHANGE_TASK_CPU_TIME, 'Change task CPU time'),
        (ACTION_CHANGE_TASK_SPLIT_RULE, 'Change split rule for task'),
        (ACTION_CHANGE_TASK_ATTRIBUTE, 'Change task attribute'),
        (ACTION_PAUSE_TASK, 'Pause task'),
        (ACTION_RESUME_TASK, 'Resume task'),
        (ACTION_TRIGGER_TASK_BROKERAGE, 'Trigger task brokerage'),
        (ACTION_AVALANCHE_TASK, 'Force avalanche for task'),
        (ACTION_REASSIGN_JOBS, 'Reassign jobs'),
        (ACTION_OBSOLETE_ENTITY, 'Obsolete task or chain'),
        (ACTION_SET_TTCR, 'Set TTC Requested (TTCR) time offset'),
        (ACTION_SET_TTCJ, 'Set TTC JEDI (TTCJ) timestamp'),
        (ACTION_REASSIGN_TASK_TO_SHARE, 'Reassign task to a new share'),
        (ACTION_RELOAD_INPUT, 'Reload input')
    )

    STATUS_RESULT_SUCCESS = 'success'
    STATUS_RESULT_ERROR = 'error'
    STATUS_RESULT_EXCEPTION = 'exception'

    STATUS_RESULT_LIST = (
        STATUS_RESULT_SUCCESS,
        STATUS_RESULT_ERROR,
        STATUS_RESULT_EXCEPTION
    )

    STATUS_TEMPLATE = {
        'result': '{{result}}',
        'error': None,
        'exception': None
    }

    created = models.DateTimeField(auto_now_add=True, help_text='Request created time')
    timestamp = models.DateTimeField(auto_now=True, help_text='Request updated time')
    action = models.CharField(max_length=32,
                              choices=ACTION_LIST,
                              help_text='Request action, one of: {0}'.format(
                                  ', '.join(['%s' % e[0] for e in ACTION_LIST])))
    owner = models.CharField(max_length=128, help_text='Request owner')
    body = models.CharField(max_length=4096, null=True, blank=True, help_text='Request parameters as a JSON string')
    status = models.CharField(max_length=1024, help_text='Request status', null=True, blank=True)

    def set_status(self, result, error=None, exception=None, data_dict=None):
        status = self.STATUS_TEMPLATE.copy()
        status['result'] = result
        status['error'] = error
        status['exception'] = exception
        if data_dict:
            status.update(data_dict)
        self.status = json.dumps(status)
        self.save()

    def get_status(self):
        return json.loads(self.status)

    def create_default_task_comment(self, body):
        params = ', '.join('{0} = \"{1}\"'.format(key, value) for key, value in body.items())
        status = self.get_status()
        if params:
            task_comment = '[{0}] action = \"{1}\", owner = \"{2}\", result = \"{3}\", parameters: {4}'.format(
                timezone.now(),
                self.action,
                self.owner,
                status['result'],
                params
            )
        else:
            task_comment = '[{0}] action = \"{1}\", owner = \"{2}\", result = \"{3}\"'.format(
                timezone.now(),
                self.action,
                self.owner,
                status['result']
            )
        if 'jedi_info' in status:
            jedi_info = status['jedi_info']
            task_comment += ' (JEDI: status_code = {0}, return_code = {1}, return_info = \"{2}\")'.format(
                jedi_info['status_code'], jedi_info['return_code'], jedi_info['return_info'])
        return task_comment

    def __unicode__(self):
        return str(self.id)

    class Meta:
        # __metaclass__ = MetaProxy
        db_name = u'deft_intr'
        db_table = u'"ATLAS_DEFT"."T_API_REQUEST"'


@receiver(post_save, sender=Request)
def request_handler(sender, **kwargs):
    request = kwargs['instance']
    if not request or request.status:
        return
    ApiServer().process_request(request)
