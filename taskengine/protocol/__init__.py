__author__ = 'Dmitry Golubkov'

import os
import ast
import json
import re
from django.template import Context, Template
from django.template.defaultfilters import stringfilter
from django import template
from deftcore.helpers import Enum
from deftcore.log import Logger

logger = Logger.get()
register = template.Library()


@register.filter(is_safe=True)
@stringfilter
def json_str(value):
    return value.replace('\\', '\\\\').replace('"', '\\"')


class Constant(object):
    def __init__(self, format_or_value, format_arg_names=None):
        self.format_or_value = format_or_value
        self.format_arg_names = format_arg_names or tuple()

    def is_dynamic(self):
        return not self.format_arg_names is tuple()


class Constants(type):
    def __new__(mcs, name, bases, namespace):
        attributes = dict()

        for attr_name in namespace.keys():
            attr = namespace[attr_name]
            if isinstance(attr, Constant):
                if isinstance(attr.format_or_value, str) or isinstance(attr.format_or_value, unicode):
                    args = list()
                    for arg_name in attr.format_arg_names:
                        arg_attr = namespace[arg_name]
                        if arg_attr.is_dynamic():
                            raise Exception('{0} class definition is incorrect'.format(name))
                        args.append(arg_attr.format_or_value)
                    attr_value = attr.format_or_value % tuple(args)
                else:
                    attr_value = attr.format_or_value
                # FIXME: implement support for protocol fixes
                attributes[attr_name] = attr_value

        for attr_name in attributes.keys():
            namespace[attr_name] = attributes[attr_name]

        cls = super(Constants, mcs).__new__(mcs, name, bases, namespace)

        return cls

    def __setattr__(cls, name, value):
        raise TypeError('Constant {0} cannot be updated'.format(name))


class TaskParamName(Enum):
    values = ['CONSTANT',
              'SKIP_EVENTS',
              'MAX_EVENTS',
              'RANDOM_SEED',
              'RANDOM_SEED_MC',
              'FIRST_EVENT',
              'DB_RELEASE',
              'INPUT',
              'INPUT_DIRECT_IO',
              'OUTPUT',
              'TXT_OUTPUT',
              'SECONDARY_INPUT_MINBIAS',
              'SECONDARY_INPUT_CAVERN',
              'SECONDARY_INPUT_ZERO_BIAS_BS',
              'LOG',
              'JOB_NUMBER',
              'FILTER_FILE',
              'TRAIN_DAOD_FILE',
              'TRAIN_DAOD_FILE_JEDI_MERGE',
              'TRAIN_OUTPUT',
              'TXT_EVENTID_OUTPUT',
              'TAR_CONFIG_OUTPUT',
              'ZIP_OUTPUT',
              'ZIP_MAP',
              'OVERLAY_FILTER_FILE',
              'HITAR_FILE']


class TaskStatus(Enum):
    values = ['TESTING',
              'WAITING',
              'FAILED',
              'BROKEN',
              'OBSOLETE',
              'ABORTED',
              'TOABORT',
              'RUNNING',
              'FINISHED',
              'DONE',
              'TORETRY']


class StepStatus(Enum):
    values = ['APPROVED',
              'NOTCHECKED',
              'WAITING']


class RequestStatus(Enum):
    values = ['APPROVED', 'PROCESSED', 'WORKING']


class Protocol(object):
    VERSION = '2.0'

    TASK_PARAM_TEMPLATES = {
        TaskParamName.CONSTANT: """{
            "type": "constant",
            "value": "{{name}}{{separator}}{{value|json_str}}"
        }""",
        TaskParamName.SKIP_EVENTS: """{
            "param_type": "number",
            "type": "template",
            "value": "{{name}}{{separator}}${SKIPEVENTS}"
        }""",
        TaskParamName.MAX_EVENTS: """{
            "param_type": "number",
            "type": "template",
            "value": "{{name}}{{separator}}${MAXEVENTS}"
        }""",
        TaskParamName.RANDOM_SEED: """{
            "offset": {{offset}},
            "param_type": "number",
            "type": "template",
            "value": "{{name}}{{separator}}${RNDMSEED}"
        }""",
        TaskParamName.RANDOM_SEED_MC: """{
            "offset": {{offset}},
            "param_type": "pseudo_input",
            "type": "template",
            "dataset": "seq_number",
            "value": "{{name}}{{separator}}${SEQNUMBER}"
        }""",
        TaskParamName.FIRST_EVENT: """{
            "offset": {{offset}},
            "param_type": "number",
            "type": "template",
            "value": "{{name}}{{separator}}${FIRSTEVENT}"
        }""",
        TaskParamName.DB_RELEASE: """{
            "dataset": "{{dataset}}",
            "param_type": "input",
            "type": "template",
            "value": "{{name}}=${DBR}"
        }""",
        TaskParamName.INPUT: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "input",
            "type": "template",
            "value": "{{name}}=${IN{{postfix}}/L}"
        }""",
        TaskParamName.INPUT_DIRECT_IO: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "input",
            "type": "template",
            "value": "{{name}}=@${IN{{postfix}}/F}"
        }""",
        TaskParamName.OUTPUT: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "output",
            "token": "ATLASDATADISK",
            "type": "template",
            "value": "{{name}}={{data_type}}.{{task_id|stringformat:\".08d\"}}._${SN}.pool.root"
        }""",
        TaskParamName.TXT_OUTPUT: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "output",
            "token": "ATLASDATADISK",
            "type": "template",
            "value": "{{name}}={{data_type}}.{{task_id|stringformat:\".08d\"}}._${SN}.tar.gz"
        }""",
        # FIXME: OverlayTest
        TaskParamName.TXT_EVENTID_OUTPUT: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "output",
            "token": "ATLASDATADISK",
            "type": "template",
            "value": "{{name}}=events.{{task_id|stringformat:\".08d\"}}._${SN}.txt"
        }""",
        TaskParamName.TAR_CONFIG_OUTPUT: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "output",
            "token": "ATLASDATADISK",
            "type": "template",
            "value": "{{name}}={{data_type}}.{{task_id|stringformat:\".08d\"}}._${SN}.tar.gz"
        }""",
        TaskParamName.ZIP_OUTPUT: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "output",
            "token": "ATLASDATADISK",
            "type": "template",
            "value": "{{name}}={{data_type}}.{{task_id|stringformat:\".08d\"}}._${SN}.zip"
        }""",
        TaskParamName.ZIP_MAP: """{
            "type": "constant",
            "value": "<ZIP_MAP>${OUTPUT{{idx}}}:${IN_DATA/L}</ZIP_MAP>"
        }""",
        # FIXME: OverlayTest
        TaskParamName.OVERLAY_FILTER_FILE: """{
            "type": "constant",
            "value": "{{name}}{{separator}}events.{{task_id|stringformat:\".08d\"}}._${SN}.txt"
        }""",
        TaskParamName.SECONDARY_INPUT_MINBIAS: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "input",
            "ratio": 0,
            "eventRatio": {{event_ratio|default:'"None"'}},
            "type": "template",
            "value": "{{name}}=${IN_MINBIAS{{postfix}}/L}"
        }""",
        TaskParamName.SECONDARY_INPUT_CAVERN: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "input",
            "ratio": 0,
            "eventRatio": {{event_ratio|default:'"None"'}},
            "type": "template",
            "value": "{{name}}=${IN_CAVERN/L}"
        }""",
        TaskParamName.SECONDARY_INPUT_ZERO_BIAS_BS: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "input",
            "ratio": 0,
            "eventRatio": {{event_ratio|default:'"None"'}},
            "type": "template",
            "value": "{{name}}=${IN_ZERO_BIAS_BS/L}"
        }""",
        TaskParamName.LOG: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "log",
            "token": "ATLASDATADISK",
            "type": "template",
            "value": "log.{{task_id|stringformat:\".08d\"}}._${SN}.job.log.tgz"
        }""",
        TaskParamName.JOB_NUMBER: """{
            "param_type": "number",
            "type": "template",
            "value": "{{name}}{{separator}}${SN}"
        }""",
        TaskParamName.FILTER_FILE: """{
            "dataset": "{{dataset}}",
            "attributes": "repeat,nosplit",
            "param_type": "input",
            "ratio": {{ratio}},
            "type": "template",
            "files": {{files}},
            "value": "{{name}}=${IN_FILTER_FILE/L}"
        }""",
        TaskParamName.HITAR_FILE: """{
            "dataset": "{{dataset}}",
            "attributes": "repeat,nosplit",
            "param_type": "input",
            "type": "template",
            "value": "{{name}}=${IN_HITAR/L}"
        }""",
        TaskParamName.TRAIN_DAOD_FILE: """{
            "param_type": "number",
            "type": "template",
            "value": "{{name}}{{separator}}{{task_id|stringformat:\".08d\"}}._${SN/P}.pool.root.1"
        }""",
        TaskParamName.TRAIN_DAOD_FILE_JEDI_MERGE: """{
            "param_type": "number",
            "type": "template",
            "value": "{{name}}{{separator}}{{task_id|stringformat:\".08d\"}}._${SN/P}.pool.root.1.panda.um"
        }""",
        TaskParamName.TRAIN_OUTPUT: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "output",
            "token": "ATLASDATADISK",
            "type": "template",
            "value": "{{name}}={{data_type}}.{{task_id|stringformat:\".08d\"}}._${SN}.pool.root.1"
        }"""
    }

    TASK_STATUS = {
        TaskStatus.TESTING: 'testing',
        TaskStatus.WAITING: 'waiting',
        TaskStatus.FAILED: 'failed',
        TaskStatus.BROKEN: 'broken',
        TaskStatus.OBSOLETE: 'obsolete',
        TaskStatus.ABORTED: 'aborted',
        TaskStatus.TOABORT: 'toabort',
        TaskStatus.RUNNING: 'running',
        TaskStatus.FINISHED: 'finished',
        TaskStatus.DONE: 'done',
        TaskStatus.TORETRY: 'toretry'
    }

    STEP_STATUS = {
        StepStatus.APPROVED: 'Approved',
        StepStatus.NOTCHECKED: 'NotChecked',
        StepStatus.WAITING: 'Waiting'
    }

    REQUEST_STATUS = {
        RequestStatus.APPROVED: 'approved',
        RequestStatus.PROCESSED: 'processed',
        RequestStatus.WORKING: 'working'
    }

    TRF_OPTIONS = {
        r'^.*_tf.py$': {'separator': '='}
    }

    def render_param(self, proto_key, param_dict):
        for key in param_dict.keys():
            if not isinstance(param_dict[key], (str, unicode, int)):
                try:
                    param_dict[key] = json.dumps(param_dict[key])
                except:
                    param_dict[key] = param_dict[key]
        default_param_dict = {'separator': '='}
        default_param_dict.update(param_dict)
        t = Template(self.TASK_PARAM_TEMPLATES[proto_key])
        param = json.loads(t.render(Context(default_param_dict, autoescape=False)))
        for key in param.keys()[:]:
            if param[key] == 'None':
                param.pop(key, None)
        return param

    def render_task(self, task_dict):
        path = '{0}{1}task.json'.format(os.path.dirname(__file__), os.path.sep)
        with open(path, 'r') as fp:
            task_template = Template(fp.read())
        proto_task = json.loads(task_template.render(Context(task_dict, autoescape=False)))
        task = {}
        for key in proto_task.keys():
            if proto_task[key] == "" or proto_task[key] == "''" or proto_task[key].lower() == 'None'.lower() or \
                    proto_task[key].lower() == '\'None\''.lower():
                continue
            task[key] = ast.literal_eval(proto_task[key])
        return task

    def is_dynamic_jobdef_enabled(self, task):
        keys = [k.lower() for k in task.keys()]
        if 'nEventsPerJob'.lower() in keys or 'nFilesPerJob'.lower() in keys:
            return False
        else:
            return True

    def is_pileup_task(self, task):
        job_params = task['jobParameters']
        for job_param in job_params:
            if re.match(r'^.*(PtMinbias|Cavern).*File.*$', str(job_param['value']), re.IGNORECASE):
                return True
        return False

    def get_simulation_type(self, step):
        if step.request.request_type.lower() == 'MC'.lower():
            if step.step_template.step.lower() == 'evgen'.lower():
                return 'notMC'
            if str(step.step_template.ctag).lower().startswith('a'):
                return 'fast'
            else:
                return 'full'
        return 'notMC'

    def get_primary_input(self, task):
        job_params = task['jobParameters']
        for job_param in job_params:
            if not 'param_type' in job_param.keys() or job_param['param_type'].lower() != 'input'.lower():
                continue
            if re.match(r'^(--)?input.*File', job_param['value'], re.IGNORECASE):
                result = re.match(r'^(--)?input(?P<intype>.*)File', job_param['value'], re.IGNORECASE)
                if not result:
                    continue
                in_type = result.groupdict()['intype']
                if in_type.lower() == 'logs'.lower() or re.match(r'^.*(PtMinbias|Cavern).*$', in_type, re.IGNORECASE):
                    continue
                return job_param
        return None

    def set_leave_log_param(self, log_param):
        log_param['token'] = TaskDefConstants.LEAVE_LOG_TOKEN
        log_param['destination'] = TaskDefConstants.LEAVE_LOG_DESTINATION
        log_param['transient'] = TaskDefConstants.LEAVE_LOG_TRANSIENT_FLAG

    def is_leave_log_param(self, log_param):
        token = None
        destination = None
        if 'token' in log_param.keys():
            token = log_param['token']
        if 'destination' in log_param.keys():
            destination = log_param['destination']
        if token == TaskDefConstants.LEAVE_LOG_TOKEN and destination == TaskDefConstants.LEAVE_LOG_DESTINATION:
            return True
        else:
            return False

    def is_evnt_filter_step(self, project_mode, task_config):
        return 'evntFilterEff'.lower() in project_mode.keys() or 'evntFilterEff' in task_config.keys()

    def serialize_task(self, task):
        return json.dumps(task, sort_keys=True)

    def deserialize_task(self, task_string):
        return json.loads(task_string)


class TaskDefConstants(object):
    __metaclass__ = Constants

    DEFAULT_PROD_SOURCE = Constant('managed')
    DEFAULT_DEBUG_PROJECT_NAME = Constant('mc12_valid')
    DEFAULT_PROJECT_MODE = Constant({'cmtconfig': 'i686-slc5-gcc43-opt', 'spacetoken': 'ATLASDATADISK'})

    # Primary Real Datasets: project.runNumber.streamName.prodStep.dataType.Version
    # Physics Containers: project.period.superdatasetName.dataType.Version
    # Monte Carlo Datasets: project.datasetNumber.physicsShort.prodStep.dataType.Version
    DEFAULT_DATA_NAME_PATTERN = Constant(r'^(.+:)?(?P<project>\w+)\.' +
                                         r'(?P<number>(\d+|\w+))\.' +
                                         r'(?P<brief>\w+)\.' +
                                         r'(?P<prod_step>\w+)\.*' +
                                         r'(?P<data_type>\w*)\.*' +
                                         r'(?P<version>\w*)' +
                                         r'(?P<container>/|$)')

    DEFAULT_EVGEN_JO_SVN_PATH_TEMPLATE = Constant(
        'svn+ssh://svn.cern.ch/reps/atlasoff/Generators/{{campaign}}JobOptions/trunk/')
    DEFAULT_EVGEN_JO_PATH_TEMPLATE = Constant('/cvmfs/atlas.cern.ch/repo/sw/Generators/{{campaign}}JobOptions/latest/')

    DEFAULT_TASK_ID_FORMAT_BASE = Constant('.08d')
    DEFAULT_TASK_ID_FORMAT = Constant('%%%s', ('DEFAULT_TASK_ID_FORMAT_BASE',))
    DEFAULT_TASK_NAME_TEMPLATE = Constant('{{project}}.{{number}}.{{brief}}.{{prod_step}}.{{version}}')
    DEFAULT_TASK_OUTPUT_NAME_TEMPLATE = Constant('{{project}}.' +
                                                 '{{number}}.' +
                                                 '{{brief}}.' +
                                                 '{{prod_step}}.' +
                                                 '{{data_type}}.' +
                                                 '{{version}}_tid{{task_id|stringformat:\"%s\"}}_00',
                                                 ('DEFAULT_TASK_ID_FORMAT_BASE',))
    DEFAULT_OUTPUT_NAME_MAX_LENGTH = 255
    DEFAULT_DB_RELEASE_DATASET_NAME_BASE = Constant('ddo.000001.Atlas.Ideal.DBRelease.v')
    DEFAULT_MINIBIAS_NPILEUP = Constant(5)
    DEFAULT_MAX_ATTEMPT = Constant(5)

    INVALID_TASK_ID = Constant(4000000)
    DEFAULT_MAX_FILES_PER_JOB = Constant(20)
    DEFAULT_MAX_NUMBER_OF_JOBS_PER_TASK = Constant(200000)
    DEFAULT_MEMORY = Constant(2000)
    DEFAULT_MEMORY_BASE = Constant(0)
    DEFAULT_SCOUT_SUCCESS_RATE = Constant(5)
    NO_ES_MIN_NUMBER_OF_EVENTS = Constant(50000)

    LEAVE_LOG_TOKEN = Constant('ddd:.*DATADISK')
    LEAVE_LOG_DESTINATION = Constant('(type=DATADISK)\(dontkeeplog=True)')
    LEAVE_LOG_TRANSIENT_FLAG = Constant(False)

    DEFAULT_CLOUD = Constant('WORLD')

    DEFAULT_ALLOWED_INPUT_EVENTS_DIFFERENCE = 10

    DEFAULT_ES_MAX_ATTEMPT = 10
    DEFAULT_ES_MAX_ATTEMPT_JOB = 10

    DEFAULT_SC_HASHTAGS = {
        'MC16a': ['MC16:MC16a', 'MC15:MC15.*', 'None', '.*MC15.*'],
        'MC16b': ['MC16:MC16b'],
        'MC16c': ['MC16:MC16c'],
        'MC16d': ['MC16:MC16d'],
        'MC16e': ['MC16:MC16e']
    }
    DEFAULT_SC_HASHTAG_SUFFIX = '_sc_102017_mixed_cont'

    DDM_ERASE_EVENT_TYPE = 'ERASE'
    DDM_ERASE_STATUS = 'erase'
    DDM_LOST_EVENT_TYPE = 'LOST'
    DDM_LOST_STATUS = 'lost'
    DDM_PROGRESS_EVENT_TYPE = 'RULE_PROGRESS'
    DATASET_DELETED_STATUS = 'Deleted'

    DEFAULT_TASK_COMMON_OFFSET_HASHTAG_FORMAT = '_tco_{0}'
