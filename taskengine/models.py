from django.db.models import CASCADE

__author__ = 'Dmitry Golubkov'

import json
import math
from django.db import models
from django.db import connections
from django.utils.dateparse import parse_datetime
from django.dispatch import receiver
from django.db.models.signals import post_init
from django.utils import timezone
from deftcore.log import Logger, get_exception_string

logger = Logger.get()

models.options.DEFAULT_NAMES += ('db_name',)


def prefetch_id(db, seq_name):
    new_id = None
    cursor = connections[db].cursor()
    try:
        query = 'select {0}.nextval from dual'.format(seq_name)
        cursor.execute(query)
        rows = cursor.fetchall()
        new_id = rows[0][0]
    finally:
        if cursor:
            cursor.close()
    return new_id


class TRequest(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='PR_ID', primary_key=True)
    manager = models.CharField(max_length=32, db_column='MANAGER', null=False)
    description = models.CharField(max_length=256, db_column='DESCRIPTION', null=True)
    ref_link = models.CharField(max_length=256, db_column='REFERENCE_LINK', null=True)
    status = models.CharField(max_length=32, db_column='STATUS', null=False)
    provenance = models.CharField(max_length=32, db_column='PROVENANCE', null=False)
    request_type = models.CharField(max_length=32, db_column='REQUEST_TYPE', null=False)
    campaign = models.CharField(max_length=32, db_column='CAMPAIGN', null=False)
    subcampaign = models.CharField(max_length=32, db_column='SUB_CAMPAIGN', null=True)
    phys_group = models.CharField(max_length=20, db_column='PHYS_GROUP', null=True)
    energy_gev = models.DecimalField(decimal_places=0, max_digits=8, db_column='ENERGY_GEV', null=False)
    project = models.CharField(max_length=30, db_column='PROJECT', null=True)
    reference = models.CharField(max_length=50, db_column='REFERENCE', null=True)
    exception = models.DecimalField(decimal_places=0, max_digits=1, db_column='EXCEPTION', null=True)
    locked = models.DecimalField(decimal_places=0, max_digits=1, db_column='LOCKED', null=True)
    is_fast = models.DecimalField(decimal_places=0, max_digits=1, db_column='IS_FAST', null=True)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id(self._meta.db_name, 'ATLAS_DEFT.T_PRODMANAGER_REQUEST_ID_SEQ')
        super(TRequest, self).save(*args, **kwargs)

    class Meta:
        db_name = 'deft_adcr'
        db_table = '"ATLAS_DEFT"."T_PRODMANAGER_REQUEST"'


class TRequestStatus(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='REQ_S_ID', primary_key=True)
    request = models.ForeignKey(TRequest, db_column='PR_ID', on_delete=models.DO_NOTHING)
    comment = models.CharField(max_length=256, db_column='COMMENT', null=True)
    owner = models.CharField(max_length=32, db_column='OWNER', null=False)
    status = models.CharField(max_length=32, db_column='STATUS', null=False)
    timestamp = models.DateTimeField(db_column='TIMESTAMP', null=False)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id(self._meta.db_name, 'ATLAS_DEFT.T_PRODMANAGER_REQ_STAT_ID_SEQ')
        super(TRequestStatus, self).save(*args, **kwargs)

    class Meta:
        db_name = 'deft_adcr'
        db_table = '"ATLAS_DEFT"."T_PRODMANAGER_REQUEST_STATUS"'


class InputRequestList(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='IND_ID', primary_key=True)
    request = models.ForeignKey(TRequest, db_column='PR_ID', on_delete=models.DO_NOTHING)
    slice = models.DecimalField(decimal_places=0, max_digits=12, db_column='SLICE', null=False)
    brief = models.CharField(max_length=150, db_column='BRIEF')
    phys_comment = models.CharField(max_length=256, db_column='PHYSCOMMENT')
    comment = models.CharField(max_length=256, db_column='SLICECOMMENT')
    input_data = models.CharField(max_length=150, db_column='INPUTDATA', null=True)
    # FIXME: to change to dataset = models.ForeignKey(ProductionDataset, db_column='INPUTDATASET',null=True)
    input_dataset = models.CharField(max_length=150, db_column='INPUTDATASET', null=True)
    project_mode = models.CharField(max_length=256, db_column='PROJECT_MODE', null=True)
    hided = models.DecimalField(decimal_places=0, max_digits=1, db_column='HIDED', null=True)
    input_events = models.DecimalField(decimal_places=0, max_digits=12, db_column='INPUT_EVENTS', null=True)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id(self._meta.db_name, 'ATLAS_DEFT.T_INPUT_DATASET_ID_SEQ')
        super(InputRequestList, self).save(*args, **kwargs)

    class Meta:
        db_name = 'deft_adcr'
        db_table = '"ATLAS_DEFT"."T_INPUT_DATASET"'


class StepTemplate(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='STEP_T_ID', primary_key=True)
    step = models.CharField(max_length=12, db_column='STEP_NAME', null=False)
    def_time = models.DateTimeField(db_column='DEF_TIME', null=False)
    status = models.CharField(max_length=12, db_column='STATUS', null=False)
    ctag = models.CharField(max_length=12, db_column='CTAG', null=False)
    priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='PRIORITY', null=False)
    cpu_per_event = models.DecimalField(decimal_places=0, max_digits=7, db_column='CPU_PER_EVENT', null=True)
    output_formats = models.CharField(max_length=80, db_column='OUTPUT_FORMATS', null=True)
    memory = models.DecimalField(decimal_places=0, max_digits=5, db_column='MEMORY', null=True)
    trf_name = models.CharField(max_length=128, db_column='TRF_NAME', null=True)
    lparams = models.CharField(max_length=2000, db_column='LPARAMS', null=True)
    vparams = models.CharField(max_length=2000, db_column='VPARAMS', null=True)
    swrelease = models.CharField(max_length=80, db_column='SWRELEASE', null=True)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id(self._meta.db_name, 'ATLAS_DEFT.T_STEP_TEMPLATE_ID_SEQ')
        super(StepTemplate, self).save(*args, **kwargs)

    class Meta:
        db_name = 'deft_adcr'
        db_table = '"ATLAS_DEFT"."T_STEP_TEMPLATE"'


class StepExecution(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='STEP_ID', primary_key=True)
    request = models.ForeignKey(TRequest, db_column='PR_ID', on_delete=models.DO_NOTHING)
    step_template = models.ForeignKey(StepTemplate, db_column='STEP_T_ID', on_delete=models.DO_NOTHING)
    status = models.CharField(max_length=12, db_column='STATUS', null=False)
    slice = models.ForeignKey(InputRequestList, db_column='IND_ID', null=False, on_delete=models.DO_NOTHING)
    priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='PRIORITY', null=False)
    step_def_time = models.DateTimeField(db_column='STEP_DEF_TIME', null=False)
    step_appr_time = models.DateTimeField(db_column='STEP_APPR_TIME', null=True)
    step_exe_time = models.DateTimeField(db_column='STEP_EXE_TIME', null=True)
    step_done_time = models.DateTimeField(db_column='STEP_DONE_TIME', null=True)
    input_events = models.DecimalField(decimal_places=0, max_digits=10, db_column='INPUT_EVENTS', null=True)
    task_config = models.CharField(max_length=2000, db_column='TASK_CONFIG', null=True)
    step_parent_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='STEP_PARENT_ID', null=True)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id(self._meta.db_name, 'ATLAS_DEFT.T_PRODUCTION_STEP_ID_SEQ')
        if not self.step_parent_id:
            self.step_parent_id = self.id
        super(StepExecution, self).save(*args, **kwargs)

    class Meta:
        db_name = 'deft_adcr'
        db_table = '"ATLAS_DEFT"."T_PRODUCTION_STEP"'


class ProductionDataset(models.Model):
    name = models.CharField(max_length=255, db_column='name', primary_key=True)
    task_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='TASKID', null=True)
    parent_task_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='PARENT_TID', null=True)
    phys_group = models.CharField(max_length=20, db_column='PHYS_GROUP', null=True)
    events = models.DecimalField(decimal_places=0, max_digits=12, db_column='EVENTS', null=True)
    files = models.DecimalField(decimal_places=0, max_digits=12, db_column='FILES', null=False)
    status = models.CharField(max_length=12, db_column='status', null=True)
    timestamp = models.DateTimeField(db_column='TIMESTAMP', null=False)
    campaign = models.CharField(max_length=32, db_column='CAMPAIGN', null=True)
    container_flag = \
        models.DecimalField(decimal_places=0, max_digits=3, db_column='CONTAINER_FLAG', default=0, null=True)
    container_time = models.DateTimeField(db_column='CONTAINER_TIME', null=True)
    ddm_timestamp = models.DateTimeField(db_column='ddm_timestamp', null=True)
    ddm_status = models.CharField(max_length=32, db_column='ddm_status', null=True)

    class Meta:
        db_name = 'deft_adcr'
        db_table = '"ATLAS_DEFT"."T_PRODUCTION_DATASET"'


class ProductionContainer(models.Model):
    name = models.CharField(max_length=150, db_column='NAME', primary_key=True)
    task_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='PARENT_TID', null=True)
    request_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='PR_ID', null=True)
    phys_group = models.CharField(max_length=20, db_column='PHYS_GROUP', null=True)
    status = models.CharField(max_length=16, db_column='STATUS', null=True)
    c_time = models.DateTimeField(db_column='C_TIME', null=False)
    d_time = models.DateTimeField(db_column='D_TIME', null=False)
    timestamp = models.DateTimeField(db_column='TIMESTAMP', null=False)
    ddm_timestamp = models.DateTimeField(db_column='DDM_TIMESTAMP', null=True)

    class Meta:
        db_name = 'deft_adcr'
        db_table = '"ATLAS_DEFT"."T_PRODUCTION_CONTAINER"'


class ProductionTask(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='TASKID', primary_key=True)
    step = models.ForeignKey(StepExecution, db_column='STEP_ID', on_delete=models.DO_NOTHING)
    request = models.ForeignKey(TRequest, db_column='PR_ID', on_delete=models.DO_NOTHING)
    parent_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='PARENT_TID', null=False)
    name = models.CharField(max_length=130, db_column='TASKNAME', null=True)
    project = models.CharField(max_length=60, db_column='PROJECT', null=True)
    dsn = models.CharField(max_length=12, db_column='DSN', null=True)
    phys_short = models.CharField(max_length=80, db_column='PHYS_SHORT', null=True)
    simulation_type = models.CharField(max_length=20, db_column='SIMULATION_TYPE', null=True)
    phys_group = models.CharField(max_length=20, db_column='PHYS_GROUP', null=True)
    provenance = models.CharField(max_length=12, db_column='PROVENANCE', null=True)
    status = models.CharField(max_length=12, db_column='STATUS', null=True)
    total_events = models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_EVENTS', null=True)
    total_req_jobs = models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_REQ_JOBS', null=True)
    total_done_jobs = models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_DONE_JOBS', null=True)
    submit_time = models.DateTimeField(db_column='SUBMIT_TIME', null=False)
    start_time = models.DateTimeField(db_column='START_TIME', null=True)
    timestamp = models.DateTimeField(db_column='TIMESTAMP', null=True)
    bug_report = models.DecimalField(decimal_places=0, max_digits=12, db_column='BUG_REPORT', null=False)
    pptimestamp = models.DateTimeField(db_column='PPTIMESTAMP', null=True)
    postproduction = models.CharField(max_length=128, db_column='POSTPRODUCTION', null=True)
    priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='PRIORITY', null=True)
    update_time = models.DateTimeField(db_column='UPDATE_TIME', null=True)
    update_owner = models.CharField(max_length=24, db_column='UPDATE_OWNER', null=True)
    comments = models.CharField(max_length=256, db_column='COMMENTS', null=True)
    inputdataset = models.CharField(max_length=150, db_column='INPUTDATASET', null=True)
    physics_tag = models.CharField(max_length=20, db_column='PHYSICS_TAG', null=True)
    vo = models.CharField(max_length=16, db_column='VO', null=True)
    prodSourceLabel = models.CharField(max_length=20, db_column='PRODSOURCELABEL', null=True)
    username = models.CharField(max_length=128, db_column='USERNAME', null=True)
    chain_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='CHAIN_TID', null=True)
    reference = models.CharField(max_length=50, db_column='REFERENCE', null=True)
    dynamic_jobdef = models.DecimalField(decimal_places=0, max_digits=1, db_column='DYNAMIC_JOB_DEFINITION', null=True)
    campaign = models.CharField(max_length=32, db_column='CAMPAIGN', null=True)
    total_req_events = models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_REQ_EVENTS', null=True)
    pileup = models.DecimalField(decimal_places=0, max_digits=1, db_column='PILEUP', null=True)
    subcampaign = models.CharField(max_length=32, db_column='SUBCAMPAIGN', null=True)
    bunchspacing = models.CharField(max_length=32, db_column='BUNCHSPACING', null=True)
    is_extension = models.DecimalField(decimal_places=0, max_digits=1, db_column='IS_EXTENSION', null=True)
    ttcr_timestamp = models.DateTimeField(db_column='TTCR_TIMESTAMP', null=True)
    ttcj_timestamp = models.DateTimeField(db_column='TTCJ_TIMESTAMP', null=True)
    ttcj_update_time = models.DateTimeField(db_column='TTCJ_UPDATE_TIME', null=True)
    end_time = models.DateTimeField(db_column='ENDTIME', null=True)
    pp_flag = models.DecimalField(decimal_places=0, max_digits=3, db_column='PPFLAG', null=True)
    pp_grace_period = models.DecimalField(decimal_places=0, max_digits=4, db_column='PPGRACEPERIOD', null=True)
    primary_input = models.CharField(max_length=250, db_column='PRIMARY_INPUT', null=True)
    ctag = models.CharField(max_length=15, db_column='CTAG', null=True)
    output_formats = models.CharField(max_length=250, db_column='OUTPUT_FORMATS', null=True)
    nfiles_to_be_used = models.DecimalField(decimal_places=0, max_digits=10, db_column='NFILESTOBEUSED', null=True)
    nfiles_used = models.DecimalField(decimal_places=0, max_digits=10, db_column='NFILESUSED', null=True)
    nfiles_finished = models.DecimalField(decimal_places=0, max_digits=10, db_column='NFILESFINISHED', null=True)
    nfiles_failed = models.DecimalField(decimal_places=0, max_digits=10, db_column='NFILESFAILED', null=True)
    nfiles_on_hold = models.DecimalField(decimal_places=0, max_digits=10, db_column='NFILESONHOLD', null=True)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id(self._meta.db_name, 'ATLAS_DEFT.T_PRODUCTION_TASK_ID_SEQ')
        super(ProductionTask, self).save(*args, **kwargs)

    @property
    def input_dataset(self):
        return TTask.objects.get(id=self.id).input_dataset

    @property
    def events_per_job(self):
        return TTask.objects.get(id=self.id).events_per_job

    @property
    def events_per_file(self):
        return TTask.objects.get(id=self.id).events_per_file

    @property
    def number_of_files(self):
        return TTask.objects.get(id=self.id).number_of_files

    @property
    def number_of_events(self):
        return TTask.objects.get(id=self.id).number_of_events

    @property
    def hashtags(self):
        return self._get_hashtags_by_task(int(self.id))

    def hashtag_exists(self, hashtag):
        hashtag_id = HashTag.objects.get(hashtag=hashtag).id
        task_id = int(self.id)
        exists = False
        cursor = None
        try:
            cursor = connections[self._meta.db_name].cursor()
            cursor.execute(
                'select TASKID,HT_ID from {0} where HT_ID={1} and TASKID={2}'.format(
                    HashTagToTask._meta.db_table, hashtag_id, task_id))
            result = cursor.fetchall()
            if result:
                exists = True
        finally:
            if cursor:
                cursor.close()
        return exists

    def set_hashtag(self, hashtag):
        hashtag_id = HashTag.objects.get(hashtag=hashtag).id
        task_id = int(self.id)
        cursor = None
        try:
            cursor = connections[self._meta.db_name].cursor()
            cursor.execute(
                'insert into {0} (HT_ID,TASKID) values ({1},{2})'.format(
                    HashTagToTask._meta.db_table, hashtag_id, task_id))
        finally:
            if cursor:
                cursor.close()

    def _get_hashtags_by_task(self, task_id):
        cursor = None
        try:
            cursor = connections[self._meta.db_name].cursor()
            cursor.execute('select HT_ID from {0} where TASKID={1}'.format(HashTagToTask._meta.db_table, task_id))
            hashtags_id = cursor.fetchall()
        finally:
            if cursor:
                cursor.close()
        hashtags = [HashTag.objects.get(id=e[0]) for e in hashtags_id]
        return hashtags

    @staticmethod
    def get_tasks_by_hashtag(hashtag):
        hashtag_id = HashTag.objects.get(hashtag=hashtag).id
        cursor = None
        try:
            cursor = connections[ProductionTask._meta.db_name].cursor()
            cursor.execute('select TASKID from {0} where HT_ID={1} order by TASKID desc'.format(
                HashTagToTask._meta.db_table,
                hashtag_id)
            )
            tasks = cursor.fetchall()
        finally:
            if cursor:
                cursor.close()
        return [ProductionTask.objects.get(id=x[0]) for x in tasks]

    def _get_datetime_utc(self, field_name, task_id):
        cursor = None
        try:
            cursor = connections[self._meta.db_name].cursor()
            cursor.execute(
                "select TO_CHAR({0}) from {1} where TASKID={2}".format(field_name, self._meta.db_table, task_id))
            rows = cursor.fetchall()
            return rows[0][0]
        finally:
            if cursor:
                cursor.close()

    class Meta:
        db_name = 'deft_adcr'
        db_table = '"ATLAS_DEFT"."T_PRODUCTION_TASK"'


class HashTag(models.Model):
    HASHTAG_TYPE = (
        ('UD', 'User defined'),
        ('KW', 'Key word'),
    )

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='HT_ID', primary_key=True)
    hashtag = models.CharField(max_length=80, db_column='HASHTAG')
    type = models.CharField(max_length=2, db_column='TYPE', choices=HASHTAG_TYPE)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id(self._meta.db_name, 'ATLAS_DEFT.T_HASHTAG_ID_SEQ')
        super(HashTag, self).save(*args, **kwargs)

    def __str__(self):
        return self.hashtag

    class Meta:
        db_name = 'deft_adcr'
        db_table = '"ATLAS_DEFT"."T_HASHTAG"'


class HashTagToTask(models.Model):
    task = models.ForeignKey(ProductionTask, db_column='TASKID', on_delete=models.DO_NOTHING)
    hashtag = models.ForeignKey(HashTag, db_column='HT_ID', on_delete=models.DO_NOTHING)

    def save(self, *args, **kwargs):
        raise NotImplementedError()

    def create_relation(self):
        pass

    class Meta:
        db_name = 'deft_adcr'
        db_table = '"ATLAS_DEFT"."T_HT_TO_TASK"'


class TTask(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='TASKID', primary_key=True)
    parent_tid = models.DecimalField(decimal_places=0, max_digits=12, db_column='PARENT_TID', null=True)
    status = models.CharField(max_length=12, db_column='STATUS', null=True)
    total_done_jobs = models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_DONE_JOBS', null=True)
    total_req_jobs = models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_REQ_JOBS', null=True)
    submit_time = models.DateTimeField(db_column='SUBMIT_TIME', null=False)
    start_time = models.DateTimeField(db_column='START_TIME', null=True)
    timestamp = models.DateTimeField(db_column='TIMESTAMP', null=True)
    jedi_task_param = models.TextField(db_column='JEDI_TASK_PARAMETERS', null=True)
    vo = models.CharField(max_length=16, db_column='VO', null=True)
    prodSourceLabel = models.CharField(max_length=20, db_column='PRODSOURCELABEL', null=True)
    taskname = models.CharField(max_length=128, db_column='TASKNAME', null=True)
    username = models.CharField(max_length=128, db_column='USERNAME', null=True)
    chain_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='CHAIN_TID', null=True)
    total_events = models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_EVENTS', null=True)

    def get_id(self):
        return prefetch_id(self._meta.db_name, 'ATLAS_DEFT.PRODSYS2_TASK_ID_SEQ')

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = self.get_id()
        super(TTask, self).save(*args, **kwargs)

    def _get_task_params(self):
        return json.loads(self.jedi_task_param)

    def _get_dataset(self, ds_type):
        if ds_type not in ['input', 'output']:
            return
        params = self._get_task_params()
        job_params = params.get('jobParameters')
        if not job_params:
            return
        for param in job_params:
            param_type, dataset = [param.get(x) for x in ('param_type', 'dataset')]
            if (param_type == ds_type) and (dataset is not None):
                return dataset
        return

    def get_job_parameter(self, value, parameter_key):
        params = self._get_task_params()
        job_params = params.get('jobParameters')
        if not job_params:
            return None
        for param in job_params:
            if ('value' in param) and ('%s=' % value in param['value']) and (parameter_key in param):
                return param[parameter_key]
        return None

    @property
    def input_dataset(self):
        return self._get_dataset('input')

    @property
    def output_dataset(self):
        return self._get_dataset('output')

    @property
    def events_per_job(self):
        value = 0
        params = self._get_task_params()
        if params:
            value = params.get('nEventsPerJob', 0)
        if not value:
            if self.total_done_jobs:
                value = math.ceil(float(self.total_events) / float(self.total_done_jobs))
        return value

    @property
    def events_per_file(self):
        value = 0
        params = self._get_task_params()
        if params:
            value = params.get('nEventsPerInputFile', 0)
        if not value:
            if self.parent_tid and self.parent_tid != self.id:
                parent_task = TTask.objects.get(id=self.parent_tid)
                value = parent_task.events_per_job
        return value

    @property
    def number_of_files(self):
        value = 0
        params = self._get_task_params()
        if params:
            value = params.get('nFiles', 0)
        return value

    @property
    def number_of_events(self):
        value = 0
        params = self._get_task_params()
        if params:
            value = params.get('nEvents', 0)
        return value

    class Meta:
        db_name = 'deft_adcr'
        db_table = '"ATLAS_DEFT"."T_TASK"'

class StepAction(models.Model):

    STAGING_ACTION = 5

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='STEP_ACTION_ID', primary_key=True)
    request = models.ForeignKey(TRequest,  db_column='PR_ID', on_delete=CASCADE)
    step = models.DecimalField(decimal_places=0, max_digits=12, db_column='STEP_ID')
    action = models.DecimalField(decimal_places=0, max_digits=12, db_column='ACTION_TYPE')
    create_time = models.DateTimeField(db_column='SUBMIT_TIME')
    execution_time = models.DateTimeField(db_column='EXEC_TIME')
    done_time = models.DateTimeField(db_column='DONE_TIME')
    message = models.CharField(max_length=2000, db_column='MESSAGE')
    attempt = models.DecimalField(decimal_places=0, max_digits=12, db_column='ATTEMPT')
    status = models.CharField(max_length=20, db_column='STATUS', null=True)
    config = models.CharField(max_length=2000, db_column='CONFIG')

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id(self._meta.db_name,'T_STEP_ACTION_SQ')
        super(StepAction, self).save(*args, **kwargs)

    def set_config(self, update_dict):
        if not self.config:
            self.config = ''
            currrent_dict = {}
        else:
            currrent_dict = json.loads(self.config)
        currrent_dict.update(update_dict)
        self.config = json.dumps(currrent_dict)

    def remove_config(self, key):
        if self.config:
            currrent_dict = json.loads(self.config)
            if key in currrent_dict:
                currrent_dict.pop(key)
                self.config = json.dumps(currrent_dict)

    def get_config(self, field = None):
        return_dict = {}
        try:
            return_dict = json.loads(self.config)
        except:
            pass
        if field:
            return return_dict.get(field,None)
        else:
            return return_dict


    class Meta:
        db_name = 'deft_adcr'
        db_table = '"T_STEP_ACTION"'

class TTrfConfig(models.Model):
    tag = models.CharField(max_length=1, db_column='TAG', null=False)
    cid = models.DecimalField(decimal_places=0, max_digits=5, db_column='CID', primary_key=True)
    trf = models.CharField(max_length=80, db_column='TRF', null=True)
    lparams = models.CharField(max_length=2048, db_column='LPARAMS', null=True)
    vparams = models.CharField(max_length=4000, db_column='VPARAMS', null=True)
    trf_version = models.CharField(max_length=40, db_column='TRFV', null=True)
    status = models.CharField(max_length=12, db_column='STATUS', null=True)
    ami_flag = models.DecimalField(decimal_places=0, max_digits=10, db_column='AMI_FLAG', null=True)
    created_by = models.CharField(max_length=60, db_column='CREATEDBY', null=True)
    input = models.CharField(max_length=20, db_column='INPUT', null=True)
    prod_step = models.CharField(max_length=12, db_column='STEP', null=True)
    formats = models.CharField(max_length=256, db_column='FORMATS', null=True)
    cache = models.CharField(max_length=32, db_column='CACHE', null=True)
    cpu_per_event = models.DecimalField(decimal_places=0, max_digits=5, db_column='CPU_PER_EVENT', null=True)
    memory = models.DecimalField(decimal_places=0, max_digits=5, db_column='MEMORY')
    priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='PRIORITY')
    events_per_job = models.DecimalField(decimal_places=0, max_digits=10, db_column='EVENTS_PER_JOB')
    comment = models.CharField(max_length=2048, db_column='COMMENT_', null=True)

    class Meta:
        db_name = 'grisli_adcr_panda'
        db_table = '"ATLAS_GRISLI"."T_TRF_CONFIG"'


class TTaskRequest(models.Model):
    reqid = models.DecimalField(decimal_places=0, max_digits=10, db_column='REQID', primary_key=True, default=0)
    project = models.CharField(max_length=60, db_column='PROJECT', null=True)
    inputdataset = models.CharField(max_length=150, db_column='INPUTDATASET', null=True)
    taskname = models.CharField(max_length=130, db_column='TASKNAME', null=True)
    formats = models.CharField(max_length=256, db_column='FORMATS', null=True)
    total_events = models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_EVENTS', null=True)
    events_per_file = models.DecimalField(decimal_places=0, max_digits=8, db_column='EVENTS_PER_FILE', null=True,
                                          default=1000)
    status = models.CharField(max_length=12, db_column='STATUS', null=True, default='pending')
    total_req_jobs = models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_REQ_JOBS', default=0)
    total_done_jobs = models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_DONE_JOBS', default=0)
    ctag = models.CharField(max_length=8, db_column='CTAG', null=True)

    class Meta:
        db_name = 'grisli_adcr_panda'
        db_table = '"ATLAS_GRISLI"."T_TASK_REQUEST"'


# noinspection PyBroadException
class Task(ProductionTask):
    def _get_jedi_task_params(self):
        if self.jedi_task:
            return json.loads(self.jedi_task.jedi_task_param)
        else:
            return None

    def _get_jedi_task_status(self):
        if self.jedi_task:
            return self.jedi_task.status
        else:
            return None

    def _get_task_config(self):
        try:
            return json.loads(self.step.task_config)
        except Exception:
            return None

    def _get_formats(self):
        return self.step.step_template.output_formats

    def _get_destination_token(self):
        try:
            task_config = json.loads(self.step.task_config)
            if 'token' in list(task_config.keys()):
                token = task_config['token']
                if 'dst:' in token:
                    return token.split('dst:')[-1]
            return None
        except Exception:
            return None

    def _get_slice(self):
        return self.step.slice.slice

    def _get_hidden(self):
        return bool(self.step.slice.hided)

    def _get_has_pileup(self):
        if self.pileup is not None:
            return bool(self.pileup)
        else:
            return self.pileup

    def _get_datetime_utc(self, field_name, task_id):
        timestamp = super(Task, self)._get_datetime_utc(field_name, task_id)
        if not timestamp:
            return None
        return parse_datetime(timestamp).replace(microsecond=0)

    def _get_start_time_utc(self):
        return self._get_datetime_utc('START_TIME', int(self.id))

    def _get_end_time_utc(self):
        return self._get_datetime_utc('ENDTIME', int(self.id))

    def _get_submit_time_utc(self):
        return self._get_datetime_utc('SUBMIT_TIME', int(self.id))

    def _get_ttcr_timestamp_utc(self):
        return self._get_datetime_utc('TTCR_TIMESTAMP', int(self.id))

    jedi_task_params = property(_get_jedi_task_params)
    jedi_task_status = property(_get_jedi_task_status)
    task_config = property(_get_task_config)
    formats = property(_get_formats)
    destination_token = property(_get_destination_token)
    slice = property(_get_slice)
    hidden = property(_get_hidden)
    has_pileup = property(_get_has_pileup)
    start_time_utc = property(_get_start_time_utc)
    end_time_utc = property(_get_end_time_utc)
    submit_time_utc = property(_get_submit_time_utc)
    ttcr_timestamp_utc = property(_get_ttcr_timestamp_utc)

    class Meta:
        ordering = ["-id"]
        proxy = True
        db_name = 'deft_adcr'


# noinspection PyBroadException
@receiver(post_init, sender=Task)
def task_post_init(sender, **kwargs):
    self = kwargs['instance']

    try:
        self.jedi_task = TTask.objects.get(id=self.id)
    except Exception:
        self.jedi_task = None

    try:
        if not self.total_req_events:
            self.total_req_events = int(self.step.input_events or 0)
    except Exception:
        self.total_req_events = 0

    try:
        self.slice_input_events = int(self.step.slice.input_events or 0)
    except Exception:
        self.slice_input_events = 0


class TRequestProxy(TRequest):
    class Meta:
        ordering = ['-id']
        proxy = True
        db_name = 'deft_adcr'


# noinspection PyBroadException
@receiver(post_init, sender=TRequestProxy)
def t_request_proxy_post_init(sender, **kwargs):
    self = kwargs['instance']
    self.is_error = bool(self.exception)
    self.creation_time = None
    self.approval_time = None
    if self.id:
        result = TRequestStatus.objects.filter(request__id=self.id).order_by('timestamp')
        if len(result) > 0:
            self.creation_time = result[0].timestamp
        result = TRequestStatus.objects.filter(request__id=self.id, status='approved').order_by('-timestamp')
        if len(result) > 0:
            self.approval_time = result[0].timestamp
    try:
        self.evgen_steps = list()
        if not self.evgen_steps:
            input_slices = InputRequestList.objects.filter(request__id=self.id).order_by('slice')
            for input_slice in input_slices:
                try:
                    if input_slice.input_data and not input_slice.hided:
                        if '/' not in input_slice.input_data:
                            dsid = int(input_slice.input_data.split('.')[1])
                            brief = input_slice.input_data.split('.')[2]
                        else:
                            dsid = int(input_slice.input_data.split('/')[0])
                            brief = input_slice.input_data.split('/')[1].split('.')[1]
                        evgen_steps = StepExecution.objects.filter(request__id=self.id,
                                                                   step_template__step__iexact='evgen',
                                                                   slice__slice=input_slice.slice)
                        if evgen_steps:
                            for evgen_step in evgen_steps:
                                self.evgen_steps.append({'dsid': dsid,
                                                         'brief': brief,
                                                         'input_events': evgen_step.input_events,
                                                         'jo': evgen_step.slice.input_data,
                                                         'ctag': evgen_step.step_template.ctag,
                                                         'slice': int(evgen_step.slice.slice)})
                        else:
                            self.evgen_steps.append({'dsid': dsid,
                                                     'brief': brief,
                                                     'jo': input_slice.input_data,
                                                     'slice': int(input_slice.slice)})
                except Exception as ex:
                    logger.exception('Exception occurred: {0}'.format(ex))
    except:
        logger.exception('Exception occurred: {0}'.format(get_exception_string()))
        self.evgen_steps = None


class TProject(models.Model):
    project = models.CharField(max_length=30, db_column='PROJECT', primary_key=True)
    status = models.CharField(max_length=8, db_column='STATUS')
    description = models.CharField(max_length=500, db_column='DESCRIPTION', null=True)
    timestamp = models.DecimalField(decimal_places=0, max_digits=10, db_column='TIMESTAMP')

    class Meta:
        db_name = 'deft_adcr'
        db_table = '"ATLAS_DEFT"."T_PROJECTS"'


class TDataFormat(models.Model):
    name = models.CharField(max_length=64, db_column='NAME', primary_key=True)
    description = models.CharField(max_length=256, db_column='DESCRIPTION', null=True)

    class Meta:
        db_name = 'deft_adcr'
        db_table = '"ATLAS_DEFT"."T_DATA_FORMAT"'


class TStepProxy(StepExecution):
    class Meta:
        ordering = ['-id']
        proxy = True
        db_name = 'deft_adcr'


@receiver(post_init, sender=TStepProxy)
def t_step_proxy_post_init(sender, **kwargs):
    self = kwargs['instance']
    if self.id:
        self.ctag = self.step_template.ctag
        self.slice_n = self.slice.slice
        self.request_id = self.request.id


class JEDIDataset(models.Model):
    task_id = models.DecimalField(decimal_places=0, max_digits=11, db_column='JEDITASKID', primary_key=True)
    dataset_id = models.DecimalField(decimal_places=0, max_digits=11, db_column='DATASETID', null=False)
    dataset_name = models.CharField(max_length=255, db_column='DATASETNAME', null=False)
    type = models.CharField(max_length=20, db_column='TYPE', null=False)
    number_files_finished = models.DecimalField(decimal_places=0, max_digits=10, db_column='NFILESFINISHED')
    nfiles = models.DecimalField(decimal_places=0, max_digits=10, db_column='NFILES')

    class Meta:
        db_name = 'deft_adcr'
        db_table = '"ATLAS_PANDA"."JEDI_DATASETS"'


class JEDIDatasetContent(models.Model):
    dataset_id = models.DecimalField(decimal_places=0, max_digits=11, db_column='datasetid', primary_key=True)
    task_id = models.DecimalField(decimal_places=0, max_digits=11, db_column='JEDITASKID', null=False)
    status = models.CharField(max_length=64, db_column='STATUS', null=False)
    filename = models.CharField(max_length=256, db_column='lfn', null=False)

    def save(self, *args, **kwargs):
        return

    class Meta:
        db_name = 'panda_adcr'
        db_table = '"ATLAS_PANDA"."JEDI_DATASET_CONTENTS"'


class InstalledSW(models.Model):
    site_id = models.CharField(max_length=60, db_column='SITEID', primary_key=True)
    cloud = models.CharField(max_length=10, db_column='CLOUD', null=True)
    release = models.CharField(max_length=10, db_column='RELEASE', null=True)
    cache = models.CharField(max_length=40, db_column='CACHE', null=True)
    cmtconfig = models.CharField(max_length=40, db_column='CMTCONFIG', null=True)

    class Meta:
        db_name = 'deft_adcr'
        db_table = '"ATLAS_PANDAMETA"."INSTALLEDSW"'


class AuthUser(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='ID', primary_key=True)
    username = models.CharField(max_length=50, db_column='USERNAME')
    email = models.CharField(max_length=100, db_column='EMAIL')

    class Meta:
        db_name = 'deft_adcr'
        db_table = '"ATLAS_DEFT"."AUTH_USER"'


class OpenEnded(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='OE_ID', primary_key=True)
    request = models.ForeignKey(TRequest, db_column='PR_ID', on_delete=models.DO_NOTHING)
    status = models.CharField(max_length=20, db_column='STATUS', null=True)

    class Meta:
        db_name = 'deft_adcr'
        db_table = '"ATLAS_DEFT"."T_OPEN_ENDED"'


class PhysicsContainer(models.Model):
    name = models.CharField(max_length=150, db_column='NAME', primary_key=True)
    created = models.DateTimeField(db_column='CREATED', null=False)
    last_modified = models.DateTimeField(db_column='LAST_MODIFIED', null=True)
    username = models.CharField(max_length=60, db_column='USERNAME', null=False)
    project = models.CharField(max_length=40, db_column='PROJECT', null=True)
    data_type = models.CharField(max_length=40, db_column='DATA_TYPE', null=True)
    run_number = models.CharField(max_length=15, db_column='RUN_NUMBER', null=True)
    stream_name = models.CharField(max_length=40, db_column='STREAM_NAME', null=True)
    prod_step = models.CharField(max_length=15, db_column='PROD_STEP', null=True)

    class Meta:
        db_name = 'deft_adcr'
        db_table = '"ATLAS_DEFT"."T_PHYSICS_CONTAINER"'


class ProductionTag(models.Model):
    name = models.CharField(max_length=15, db_column='NAME', primary_key=True)
    trf_name = models.CharField(max_length=60, db_column='TRF_NAME', null=False)
    trf_cache = models.CharField(max_length=60, db_column='TRF_CACHE', null=False)
    trf_release = models.CharField(max_length=60, db_column='TRF_RELEASE', null=False)
    tag_parameters = models.TextField(db_column='TAG_PARAMETERS', null=False)
    username = models.CharField(max_length=60, db_column='USERNAME', null=False)
    created = models.DateTimeField(db_column='CREATED', null=False)
    task_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='TASKID', null=False)
    step_template_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='STEP_T_ID', null=False)

    class Meta:
        db_name = 'deft_adcr'
        db_table = '"ATLAS_DEFT"."T_PRODUCTION_TAG"'


class TConfig(models.Model):
    app = models.CharField(max_length=64, db_column='APP', null=False)
    component = models.CharField(max_length=64, db_column='COMPONENT', null=False)
    key = models.CharField(max_length=64, db_column='KEY', null=False, primary_key=True)
    value = models.CharField(max_length=1024, db_column='VALUE', null=False)
    type = models.CharField(max_length=32, db_column='TYPE', null=False)
    description = models.CharField(max_length=256, db_column='DESCRIPTION')
    timestamp = models.DateTimeField(db_column='TIMESTAMP', null=False)

    def save(self, *args, **kwargs):
        self.type = type(self.value).__name__
        self.timestamp = timezone.now()
        super(TConfig, self).save(args, kwargs)

    @staticmethod
    def get_ttcr(project, prod_step, provenance):
        key = '{0}.{1}.{2}'.format(project, prod_step.lower(), provenance)
        ttcr = TConfig.objects.filter(app='deftcore', component='ttcr', key=key)
        if len(ttcr) == 0:
            return 0
        return int(ttcr[0].value)

    @staticmethod
    def set_ttcr(offsets):
        for key in list(offsets.keys()):
            if type(offsets[key]) != int:
                raise Exception('Wrong format of TTCR time offsets package')
            param = TConfig(app='deftcore', component='ttcr', key=key, value=offsets[key])
            param.save()

    @staticmethod
    def get(app, component, key):
        param = TConfig.objects.filter(app=app, component=component, key=key)
        if len(param) == 0:
            return None
        module_ = __import__('__builtin__')
        cls = getattr(module_, param[0].type)
        return cls(param[0].value)

    class Meta:
        unique_together = (('app', 'component', 'key'),)
        db_name = 'deft_adcr'
        db_table = '"ATLAS_DEFT"."T_CONFIG"'


class DatasetStaging(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='DATASET_STAGING_ID', primary_key=True)
    dataset = models.CharField(max_length=256, db_column='DATASET', null=False)
    status = models.CharField(max_length=20, db_column='STATUS', null=True)
    tape_status_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='TAPE_STATUS_ID', null=True)
    staged_files = models.DecimalField(decimal_places=0, max_digits=12, db_column='STAGED_FILES', null=True)
    start_time = models.DateTimeField(db_column='START_TIME', null=True)
    end_time = models.DateTimeField(db_column='END_TIME', null=True)
    rse = models.CharField(max_length=100, db_column='RSE', null=True)
    total_files = models.DecimalField(decimal_places=0, max_digits=12, db_column='TOTAL_FILES', null=True)
    update_time = models.DateTimeField(db_column='UPDATE_TIME', null=True)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id(self._meta.db_name, 'ATLAS_DEFT.T_DATASET_STAGING_SEQ')
        super(DatasetStaging, self).save(*args, **kwargs)

    class Meta:
        db_name = 'deft_adcr'
        db_table = '"ATLAS_DEFT"."T_DATASET_STAGING"'
