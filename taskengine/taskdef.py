__author__ = 'Dmitry Golubkov'

import re
import json
import subprocess
import csv
import StringIO
import ast
import datetime
import copy
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Q
from django.template import Context, Template
from django.utils import timezone
from django.core.mail import send_mail
from distutils.version import LooseVersion
from taskengine.models import StepExecution, TRequest, InputRequestList, TRequestStatus, ProductionTask, TTask, \
    TTaskRequest, JediDataset, InstalledSW, AuthUser, OpenEnded, ProductionDataset, HashTag, TConfig
from taskengine.protocol import Protocol, StepStatus, TaskParamName, TaskDefConstants, RequestStatus, TaskStatus
from taskengine.taskreg import TaskRegistration
from taskengine.metadata import AMIClient
from taskengine.rucioclient import RucioClient
from taskengine.agisclient import AGISClient
from deftcore.settings import REQUEST_GRACE_PERIOD
from deftcore.log import Logger, get_exception_string
from deftcore.jira import JIRAClient

logger = Logger.get()


class NotEnoughEvents(Exception):
    pass


class TaskDuplicateDetected(Exception):
    def __init__(self, previous_task_id, reason_code, **kwargs):
        prefix = ', '.join(['{0} = {1}'.format(name, value) for name, value in kwargs.items()])
        message = '[Check duplicates] The task is rejected, previous_task = {0}, reason_code = {1}' \
            .format(previous_task_id, reason_code)
        if prefix:
            message = '[{0}] {1}'.format(prefix, message)
        super(TaskDuplicateDetected, self).__init__(message)


class NoMoreInputFiles(Exception):
    pass


class ParentTaskInvalid(Exception):
    def __init__(self, parent_task_id, task_status):
        message = 'Parent task {0} is {1}'.format(parent_task_id, task_status)
        super(ParentTaskInvalid, self).__init__(message)


class InputLostFiles(Exception):
    def __init__(self, dsn):
        message = 'Input {0} has lost files'.format(dsn)
        super(InputLostFiles, self).__init__(message)


class NumberOfFilesUnavailable(Exception):
    def __init__(self, dataset, ex_message=None):
        message = \
            '[Check duplicates] The task is rejected. Number of files is unavailable (dataset = {0})'.format(dataset)
        if ex_message:
            message = '{0}. {1}'.format(message, ex_message)
        super(NumberOfFilesUnavailable, self).__init__(message)


class UniformDataException(Exception):
    def __init__(self, dataset_name, events_per_file, number_events, number_files, config_events_per_file,
                 parent_events_per_job, parent_task_id):
        message = \
            'The task is rejected because of inconsistency. ' + \
            'nEventsPerInputFile={0} does not match to nEventsPerJob={1} of the parent (taskId={2}). '.format(
                config_events_per_file, parent_events_per_job, parent_task_id) + \
            'DDM ({0}): nEventsPerInputFile={1}, events={2}, files={3}'.format(
                dataset_name, events_per_file, number_events, number_files)
        super(UniformDataException, self).__init__(message)


class MaxJobsPerTaskLimitExceededException(Exception):
    def __init__(self, number_of_jobs):
        message = 'The task is rejected. The limit of number of jobs per task ({0}) is exceeded. '.format(
            TaskDefConstants.DEFAULT_MAX_NUMBER_OF_JOBS_PER_TASK) + \
                  'Expected number of jobs for this task is {0}'.format(number_of_jobs)
        super(MaxJobsPerTaskLimitExceededException, self).__init__(message)


class TaskConfigurationException(Exception):
    def __init__(self, message):
        super(TaskConfigurationException, self).__init__(message)


class TaskSmallEventsException(Exception):
    def __init__(self, number_of_events):
        message = 'The task is rejected. ' + \
                  'Too few events will be produced to guarantee that the pileup distribution is correct ({0}). '.format(
                      number_of_events) + 'Use isSmallEvents=yes in project_mode to force it'
        super(TaskSmallEventsException, self).__init__(message)


class UnknownSiteException(Exception):
    def __init__(self, site_name):
        message = 'The site "{0}" is unknown to AGIS'.format(site_name)
        super(UnknownSiteException, self).__init__(message)


class TaskDefineOnlyException(Exception):
    def __init__(self, url):
        message = 'The task parameters are defined: {0}'.format(url)
        super(TaskDefineOnlyException, self).__init__(message)


class OutputNameMaxLengthException(Exception):
    def __init__(self, output_name):
        message = 'The task is rejected. The output name "{0}" has length {1} but max allowed length is {2}'.format(
            output_name, len(output_name), TaskDefConstants.DEFAULT_OUTPUT_NAME_MAX_LENGTH)
        super(OutputNameMaxLengthException, self).__init__(message)


class NoRequestedCampaignInput(Exception):
    pass


class InvalidMergeException(Exception):
    def __init__(self, dsn, tag_name):
        message = 'The task is rejected. Merging with ratio 1:1 is skipped (dsn = {0}, tag = {1})'.format(dsn, tag_name)
        super(InvalidMergeException, self).__init__(message)


class UnmergedInputProcessedException(Exception):
    def __init__(self, task_id):
        message = 'The task is rejected. Unmerged input is already processed (task_id = {0})'.format(task_id)
        super(UnmergedInputProcessedException, self).__init__(message)


class MergedInputProcessedException(Exception):
    def __init__(self, task_id):
        message = 'The task is rejected. Merged input is already processed (task_id = {0})'.format(task_id)
        super(MergedInputProcessedException, self).__init__(message)


class WrongCacheVersionUsedException(Exception):
    def __init__(self, version, data_version):
        message = 'The task is rejected. The major part of the current cache version ({0}) for derivation '. \
                      format(version) + \
                  'is not equal to the version with which the corresponding input AODs were produced ({0})'. \
                      format(data_version)
        super(WrongCacheVersionUsedException, self).__init__(message)


class EmptyDataset(Exception):
    pass


class TaskDefinition(object):
    def __init__(self):
        self.protocol = Protocol()
        self.task_reg = TaskRegistration()
        self.ami_client = AMIClient()
        self.rucio_client = RucioClient()
        self.agis_client = AGISClient()

    def _get_usergroup(self, step):
        return "%s_%s" % (step.request.provenance, step.request.phys_group)

    def _get_project(self, step):
        project = step.request.project
        if not project:
            project = TaskDefConstants.DEFAULT_DEBUG_PROJECT_NAME
        return project

    def _parse_project_mode(self, project_mode_string):
        project_mode_dict = dict()
        for option in project_mode_string.replace(' ', '').split(";"):
            if not option:
                continue
            if not '=' in option:
                raise Exception('The project_mode option \"{0}\" has invalid format. '.format(option) +
                                'Expected format is \"optionName=optionValue\"')
            project_mode_dict.update({option.split("=")[0].lower(): option.split("=")[1]})
        return project_mode_dict

    def _get_task_config(self, step):
        task_config = dict()
        if step.task_config:
            content = json.loads(step.task_config)
            for key in content.keys():
                if content[key] is None or content[key] == '':
                    continue
                task_config.update({key: content[key]})
        return task_config

    def _set_task_config(self, step, task_config):
        step.task_config = json.dumps(task_config)
        step.save(update_fields=['task_config'])

    def _is_cmtconfig_exist(self, cache, cmtconfig):
        installed_cmtconfig_list = \
            InstalledSW.objects.filter(cache=cache, cmtconfig=cmtconfig).values_list('cmtconfig', flat=True).distinct()
        if bool(installed_cmtconfig_list):
            return True
        else:
            agis_cmtconfig_list = self.agis_client.get_cmtconfig(cache)
            return cmtconfig in agis_cmtconfig_list

    def _get_cmtconfig_list(self, cache):
        installed_cmtconfig_list = \
            InstalledSW.objects.filter(cache=cache).values_list('cmtconfig', flat=True).distinct()
        agis_cmtconfig_list = self.agis_client.get_cmtconfig(cache)
        cmtconfig_list = set(list(installed_cmtconfig_list) + agis_cmtconfig_list)
        return list(cmtconfig_list)

    def _get_project_mode(self, step, cache=None, use_nightly_release=None):
        """
        :param step: object of StepExecution
        :param cache: string in format 'CacheName-CacheRelease', for example, 'AtlasProduction-19.2.1.2'
        :return: project_mode dict
        """
        project_mode = dict()

        task_config = self._get_task_config(step)
        if 'project_mode' in task_config.keys():
            project_mode.update(self._parse_project_mode(task_config['project_mode']))

        skip_cmtconfig_check = False
        if 'skipCMTConfigCheck'.lower() in project_mode.keys():
            option_value = str(project_mode['skipCMTConfigCheck'.lower()])
            if option_value.lower() == 'yes'.lower():
                skip_cmtconfig_check = True

        cmtconfig = ''
        if 'cmtconfig' in project_mode.keys():
            cmtconfig = project_mode['cmtconfig']
            if cache and not skip_cmtconfig_check:
                if not self._is_cmtconfig_exist(cache, cmtconfig):
                    available_cmtconfig_list = self._get_cmtconfig_list(cache)
                    raise Exception(
                        'cmtconfig \"%s\" specified by user is not exist in cache \"%s\" (available: %s)' %
                        (cmtconfig, cache, str(','.join(available_cmtconfig_list)))
                    )

        if not cmtconfig and use_nightly_release:
            raise Exception('cmtconfig parameter must be specified in project_mode when nightly release is used')

        if not cmtconfig:
            project_mode['cmtconfig'] = TaskDefConstants.DEFAULT_PROJECT_MODE['cmtconfig']
            if cache:
                cmtconfig_list = self._get_cmtconfig_list(cache)
                if len(cmtconfig_list) == 1:
                    project_mode['cmtconfig'] = cmtconfig_list[0]
                else:
                    if len(cmtconfig_list) > 1:
                        value = str(','.join(cmtconfig_list))
                        raise Exception(
                            'cmtconfig is not specified but more than one cmtconfig is available ({0}).'.format(value) +
                            ' The task is rejected'
                        )
                    # prodsys1
                    # ver_parts = step.step_template.swrelease.split('.')
                    release = cache.split('-')[-1]
                    ver_parts = release.split('.')
                    ver = int(ver_parts[0]) * 1000 + int(ver_parts[1]) * 100 + int(ver_parts[2])
                    if int(ver_parts[0]) <= 13:
                        project_mode['cmtconfig'] = 'i686-slc3-gcc323-opt'
                    elif ver < 15603:
                        project_mode['cmtconfig'] = 'i686-slc4-gcc34-opt'
                    elif ver < 19003:
                        project_mode['cmtconfig'] = 'i686-slc5-gcc43-opt'
                    elif ver < 20100:
                        project_mode['cmtconfig'] = 'x86_64-slc6-gcc47-opt'
                    else:
                        project_mode['cmtconfig'] = 'x86_64-slc6-gcc48-opt'
                    if not project_mode['cmtconfig'] in cmtconfig_list:
                        if len(cmtconfig_list) > 0:
                            project_mode['cmtconfig'] = cmtconfig_list[0]
                        else:
                            raise Exception(
                                'Default cmtconfig \"%s\" is not exist in cache \"%s\" (available: %s)' %
                                (project_mode['cmtconfig'], cache, str(','.join(cmtconfig_list)))
                            )

        return project_mode

    def _get_energy(self, step, ctag):
        energy_ctag = None
        for name in ctag.keys():
            if re.match(r'^(--)?ecmEnergy$', name, re.IGNORECASE):
                energy_ctag = int(ctag[name])
        energy_req = int(step.request.energy_gev)
        if energy_ctag and energy_ctag != energy_req:
            raise Exception("Energy mismatch")
        else:
            return energy_req

    def get_step_input_data_name(self, step):
        if step.slice.input_dataset:
            return step.slice.input_dataset
        elif step.slice.input_data:
            return step.slice.input_data
        else:
            return None

    def parse_data_name(self, name):
        result = re.match(TaskDefConstants.DEFAULT_DATA_NAME_PATTERN, name)
        if not result:
            raise Exception('Invalid data name')

        data_name_dict = result.groupdict()
        data_name_dict.update({'name': name})
        return data_name_dict

    def _get_svn_output(self, svn_command):
        svn_args = ['svn']
        svn_args.extend(svn_command)
        process = subprocess.Popen(svn_args, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        return process.communicate()[0]

    def _get_evgen_input_dict(self, content):
        input_db = csv.DictReader(StringIO.StringIO(content))
        input_dict = dict()

        for i, row in enumerate(input_db):
            try:
                if row[input_db.fieldnames[0]].strip().startswith("#"):
                    continue

                try:
                    dsid = int(row[input_db.fieldnames[0]].strip())
                except:
                    continue
                energy = int(row[input_db.fieldnames[1]].strip())

                if not dsid in input_dict.keys():
                    input_dict[dsid] = dict()
                if not energy in input_dict[dsid].keys():
                    input_dict[dsid][energy] = dict()

                if row[input_db.fieldnames[2]]:
                    input_event_file = row[input_db.fieldnames[2]].strip().strip('/')
                    if len(input_event_file) > 1:
                        input_dict[dsid][energy]['inputeventfile'] = input_event_file

                if row[input_db.fieldnames[3]]:
                    input_conf_file = row[input_db.fieldnames[3]].strip().strip('/')
                    if len(input_conf_file) > 1:
                        input_dict[dsid][energy]['inputconfigfile'] = input_conf_file
            except:
                continue

        return input_dict

    def _parse_jo_file(self, content):
        lines = list()
        for line in content.splitlines():
            if line.startswith('#'):
                continue
            lines.append(line)
        return '\n'.join(lines)

    def _get_evgen_input_files(self, input_data_dict, energy, svn=False, use_containers=True, use_evgen_otf=False):
        path_template = Template("share/DSID{{number|slice:\"0:3\"}}xxx/{{file_name}}")
        job_options_file_path = path_template.render(
            Context({'number': str(input_data_dict['number']), 'file_name': input_data_dict['name']}, autoescape=False))

        evgen_input_path = 'share/evgeninputfiles.csv'

        if svn:
            root_path = Template(TaskDefConstants.DEFAULT_EVGEN_JO_SVN_PATH_TEMPLATE).render(
                Context({'campaign': str(input_data_dict['project']).upper()}, autoescape=False))
            latest_tag = ''

            svn_path = "%s%s%s" % (root_path, latest_tag, evgen_input_path)
            # FIXME
            if input_data_dict['project'].lower() == 'mc14'.lower():
                svn_path = svn_path.replace('MC14JobOptions', 'MC12JobOptions')
                path_template = Template("share/MC15Val/{{file_name}}")
                job_options_file_path = path_template.render(
                    Context({'file_name': input_data_dict['name']}, autoescape=False))
            elif input_data_dict['project'].lower() == 'mc10'.lower():
                return {}
            svn_command = ['cat', svn_path]
            evgen_input_content = self._get_svn_output(svn_command)

            svn_path = "%s%s%s" % (root_path, latest_tag, job_options_file_path)
            svn_command = ['cat', svn_path]
            job_options_file_content = self._get_svn_output(svn_command)
        else:
            root_path = Template(TaskDefConstants.DEFAULT_EVGEN_JO_PATH_TEMPLATE).render(
                Context({'campaign': str(input_data_dict['project']).upper()}, autoescape=False))

            path = "%s%s" % (root_path, evgen_input_path)
            # FIXME
            if input_data_dict['project'].lower() == 'mc14'.lower():
                path = path.replace('MC14JobOptions', 'MC12JobOptions')
                path_template = Template("share/MC15Val/{{file_name}}")
                job_options_file_path = path_template.render(
                    Context({'file_name': input_data_dict['name']}, autoescape=False))
            elif input_data_dict['project'].lower() == 'mc10'.lower():
                return {}
            try:
                with open(path, 'rb') as fp:
                    evgen_input_content = fp.read()
            except IOError:
                logger.warning("Evgen input content file %s is not found" % path)
                evgen_input_content = ''

            path = "%s%s" % (root_path, job_options_file_path)
            with open(path, 'r') as fp:
                job_options_file_content = fp.read()

        evgen_input_dict = self._get_evgen_input_dict(evgen_input_content)
        if not evgen_input_dict:
            raise Exception('evgeninputfiles.csv file is corrupted')

        params = dict()

        dsid = int(input_data_dict['number'])
        energy = int(energy)

        content = self._parse_jo_file(job_options_file_content)

        if not use_evgen_otf:
            if content.find('evgenConfig.inputconfcheck') >= 0:
                # inputGenConfFile
                try:
                    dsid_row = evgen_input_dict[dsid]
                except KeyError:
                    raise Exception("Invalid request parameter: DSID = %d" % dsid)
                try:
                    energy_row = dsid_row[energy]
                except KeyError:
                    raise Exception("Invalid request parameter: Energy = %d GeV" % energy)
                try:
                    evgen_input_container = "%s/" % energy_row['inputconfigfile']
                except KeyError:
                    raise Exception("Suitable inputconfigfile candidate not found in evgeninputfiles.csv")
                if use_containers:
                    params.update({'inputGenConfFile': [evgen_input_container]})
                else:
                    result = self.rucio_client.get_datasets_and_containers(evgen_input_container,
                                                                           datasets_contained_only=True)
                    params.update({'inputGenConfFile': result['datasets']})
            elif content.find('evgenConfig.inputfilecheck') >= 0:
                # inputGeneratorFile
                try:
                    dsid_row = evgen_input_dict[dsid]
                except KeyError:
                    raise Exception("Invalid request parameter: DSID = %d" % dsid)
                try:
                    energy_row = dsid_row[energy]
                except KeyError:
                    raise Exception("Invalid request parameter: Energy = %d GeV" % energy)
                try:
                    evgen_input_container = "%s/" % energy_row['inputeventfile']
                except KeyError:
                    raise Exception("Suitable inputeventfile candidate not found in evgeninputfiles.csv")
                if use_containers:
                    params.update({'inputGeneratorFile': [evgen_input_container]})
                else:
                    result = self.rucio_client.get_datasets_and_containers(evgen_input_container,
                                                                           datasets_contained_only=True)
                    params.update({'inputGeneratorFile': result['datasets']})
            else:
                dsid_row = evgen_input_dict.get(dsid)
                if dsid_row:
                    entry = dsid_row.get(energy)
                    if entry:
                        if entry.keys()[0] == 'inputeventfile':
                            evgen_input_container = "%s/" % entry['inputeventfile']
                            params.update({'inputGeneratorFile': [evgen_input_container]})
                        elif entry.keys()[0] == 'inputconfigfile':
                            evgen_input_container = "%s/" % entry['inputconfigfile']
                            params.update({'inputGenConfFile': [evgen_input_container]})

        events_per_job_param = 'evgenConfig.minevents'
        events_per_job = None
        if job_options_file_content.find(events_per_job_param) >= 0:
            for jo_file_content_line in job_options_file_content.splitlines():
                if jo_file_content_line.find(events_per_job_param) >= 0:
                    try:
                        if jo_file_content_line.startswith('#'):
                            continue
                        events_per_job = int(jo_file_content_line.replace(' ', '').split('=')[-1])
                        logger.info('Using nEventsPerJob from JO file: evgenConfig.minevents={0}'.format(
                            events_per_job))
                        break
                    except:
                        pass
        if events_per_job:
            params.update({'nEventsPerJob': events_per_job})

        files_per_job_param = 'evgenConfig.inputFilesPerJob'
        files_per_job = None
        if job_options_file_content.find(files_per_job_param) >= 0:
            for jo_file_content_line in job_options_file_content.splitlines():
                if jo_file_content_line.find(files_per_job_param) >= 0:
                    try:
                        if jo_file_content_line.startswith('#'):
                            continue
                        files_per_job = int(jo_file_content_line.replace(' ', '').split('=')[-1])
                        logger.info('Using nFilesPerJob from JO file: evgenConfig.inputFilesPerJob={0}'.format(
                            files_per_job))
                        break
                    except:
                        pass
        if files_per_job:
            params.update({'nFilesPerJob': files_per_job})

        return params

    def _add_input_dataset_name(self, name, params):
        input_dataset_dict = self.parse_data_name(name)
        param_name = "input%sFile" % input_dataset_dict['data_type']
        if not param_name in params.keys():
            params[param_name] = list()
        params[param_name].append(name)

    def _add_output_dataset_name(self, name, params):
        output_dataset_dict = self.parse_data_name(name)
        param_name = "output%sFile" % output_dataset_dict['data_type']
        if not param_name in params.keys():
            params[param_name] = list()
        params[param_name].append(name)

    def _get_parent_task_id_from_input(self, input_data_name):
        input_data_dict = self.parse_data_name(input_data_name)
        version = input_data_dict['version']
        result = re.match(r'^\w*_tid(?P<tid>\d*)_00$', version)
        if result:
            return int(result.groupdict()['tid'])
        else:
            return 0

    def get_input_params(self, step, first_step, restart, energy, use_containers=True, use_evgen_otf=False,
                         task_id=None):
        # returns input_params = {'inputAODFile': [...], 'inputEVNTFile': [...], ...}
        input_params = dict()

        if step.step_parent_id == step.id or (step.id == first_step.id and not restart):
            # first step - external input

            # get input from request
            input_data_name = self.get_step_input_data_name(step)

            if not input_data_name:
                return input_params

            input_data_dict = self.parse_data_name(input_data_name)

            if input_data_dict['prod_step'].lower() == 'py'.lower():
                # event generation - get input from latest JobOptions or SVN
                # inputGeneratorFile, inputGenConfFile
                input_params.update(self._get_evgen_input_files(input_data_dict, energy, use_evgen_otf=use_evgen_otf))
                job_config = "%sJobOptions/%s" % (input_data_dict['project'], input_data_name)
                input_params.update({'jobConfig': job_config})
                project_mode = self._get_project_mode(step)
                if 'nEventsPerJob'.lower() in project_mode.keys():
                    events_per_job = int(project_mode['nEventsPerJob'.lower()])
                    input_params.update({'nEventsPerJob': events_per_job})
                    logger.info("Using nEventsPerJob from project_mode: nEventsPerJob={0}".format(events_per_job))
            else:
                result = self.rucio_client.get_datasets_and_containers(input_data_name, datasets_contained_only=True)
                if use_containers and result['containers']:
                    input_data = result['containers']
                elif use_containers:
                    if not self.rucio_client.is_dsn_container(input_data_name):
                        input_data = result['datasets']
                    else:
                        datasets = self.rucio_client.list_datasets_in_container(input_data_name)
                        if not datasets:
                            raise Exception('The container {0} is empty'.format(input_data_name))
                        logger.debug('Using container {0}'.format(input_data_name))
                        input_data = [input_data_name, ]
                else:
                    input_data = result['datasets']
                for input_dataset_name in input_data:
                    self._add_input_dataset_name(input_dataset_name, input_params)
        else:
            # not first step - internal input, from previous step
            task_config = self._get_task_config(step)
            input_formats = list()
            if 'input_format' in task_config.keys():
                for format_name in task_config['input_format'].split('.'):
                    input_formats.append(format_name)
            for input_dataset_name in self.task_reg.get_step_output(step.step_parent_id, task_id=task_id):
                data_type = self.parse_data_name(input_dataset_name)['data_type']

                if data_type.lower() == 'log'.lower():
                    continue

                if input_formats:
                    if data_type in input_formats:
                        self._add_input_dataset_name(input_dataset_name, input_params)
                else:
                    self._add_input_dataset_name(input_dataset_name, input_params)

        return input_params

    def _construct_taskname(self, input_data_name, project, prod_step, ctag_name):
        input_data_dict = self.parse_data_name(input_data_name)
        version_list = list()
        old_version = input_data_dict['version']
        if old_version:
            result = re.match(r'^.*(?P<tid>_tid\d+_\d{2})$', old_version)
            if result:
                old_version = old_version.replace(result.groupdict()['tid'], '')
            version_list.extend(old_version.split('_'))
        version_list.append(ctag_name)
        version = '_'.join(version_list)
        input_data_dict.update({'project': project, 'prod_step': prod_step, 'version': version})
        name_template = Template(TaskDefConstants.DEFAULT_TASK_NAME_TEMPLATE)
        return name_template.render(Context(input_data_dict, autoescape=False))

    def _construct_output(self, input_data_dict, project, prod_step, ctag_name, data_type, task_id):
        version_list = list()
        old_version = input_data_dict['version']
        if old_version:
            result = re.match(r'^.*(?P<tid>_tid\d+_\d{2})$', old_version)
            if result:
                old_version = old_version.replace(result.groupdict()['tid'], '')
            version_list.extend(old_version.split('_'))
        version_list.append(ctag_name)
        version = '_'.join(version_list)
        input_data_dict.update({'project': project,
                                'prod_step': prod_step,
                                'version': version,
                                'data_type': data_type,
                                'task_id': task_id})
        output_template = Template(TaskDefConstants.DEFAULT_TASK_OUTPUT_NAME_TEMPLATE)
        return output_template.render(Context(input_data_dict, autoescape=False))

    def _get_output_params(self, input_data_name, output_types, project, prod_step, ctag_name, task_id):
        # returns output_params = {'outputAODFile': [...], 'outputEVNTFile': [...], ...}
        output_params = dict()
        for output_type in output_types:
            output_dataset_name = self._construct_output(self.parse_data_name(input_data_name),
                                                         project,
                                                         prod_step,
                                                         ctag_name,
                                                         output_type,
                                                         task_id)
            self._add_output_dataset_name(output_dataset_name, output_params)
        return output_params

    def _normalize_parameter_value(self, name, value, sub_steps):
        if not value:
            return value

        enclosed_value = False

        value = value.replace('%0B', ' ').replace('%2B', '+').replace('%9B', '; ').replace('%3B', ';')
        value = value.replace('"', '%8B').replace('%2C', ',')

        # FIXME
        if re.match('^(--)?asetup$', name, re.IGNORECASE) or re.match('^(--)?triggerConfig$', name, re.IGNORECASE):
            return value.replace('%8B', '"').replace('%8C', '"')

        value = value.replace('%8B', '\\"').replace('%8C', '\\"')

        if re.match('^(--)?reductionConf$', name, re.IGNORECASE):
            enclosed_value = True
        elif re.match('^(--)?validationFlags$', name, re.IGNORECASE):
            enclosed_value = True
        elif re.match('^(--)?athenaopts$', name, re.IGNORECASE):
            value = value.decode('string-escape')
        elif re.match('^(--)?extraParameter$', name, re.IGNORECASE):
            enclosed_value = True

        while value.find('\\\\"') >= 0 and not re.match('^(--)?athenaopts$', name, re.IGNORECASE):
            value = value.replace('\\\\"', '\\"')

        if value.replace('\\', '')[0] == '"' and value.replace('\\', '')[-1] == '"':

            enclosed_value = True

            if len(value) >= 2:
                if value[0:2] == '\\"':
                    # remove \\ from start if enclosed string
                    value = value[1:]
                if value[-2:] == '\\"':
                    # remove \\ from end if enclosed string
                    value = '%s"' % value[:-2]

        # if re.match('^(--)?reductionConf', name, re.IGNORECASE):
        #     enclosed_value = True
        # elif re.match('^(--)?validationFlags', name, re.IGNORECASE):
        #     enclosed_value = True
        # elif re.match('^(--)?athenaopts', name, re.IGNORECASE):
        #     value = value.decode('string-escape')

        # if re.match('^(--)?asetup', name, re.IGNORECASE):
        #     enclosed_value = True

        # escape all Linux spec chars
        if value.find(' ') >= 0 or value.find('(') >= 0 or value.find('=') >= 0 or value.find('*') >= 0 \
                or value.find(';') >= 0 or value.find('{') >= 0 or value.find('}') >= 0 or re.match(
            '^(--)?ignorePatterns', name, re.IGNORECASE):
            if not enclosed_value:
                value = '"%s"' % value

        # support for transformation sub_steps
        if not sub_steps is None:
            sub_step_exists = False
            for sub_step in sub_steps:
                if "%s:" % sub_step in value:
                    sub_step_exists = True
                    break
            if sub_step_exists:
                sub_values = list()
                sep = ' '
                # if ',' in value:
                #     sep = ','
                for sub_value in value.split(sep):
                    if len(sub_value) >= 2:
                        if sub_value[0:2] == '\\"':
                            sub_value = sub_value[1:]
                        if sub_value[-2:] == '\\"':
                            sub_value = '%s"' % sub_value[:-2]
                            # sub_step_value = sub_value.split(':', 1)[-1]
                            # if sub_step_value[0:2] == '\\"' and sub_step_value[-2:] == '\\"':
                            #     sub_step_value = sub_step_value[1:]
                            #     sub_step_value = '%s"' % sub_step_value[:-2]
                            # sub_value = sub_value.split(':')[0] + ':' + sub_step_value
                    sub_values.append(sub_value)
                value = ' '.join(sub_values)

        return value

    def _get_parameter_value(self, name, source_dict, sub_steps=None):
        name = name.lower()
        for key in source_dict.keys():
            param_name_prefix = '--'
            key_name = key
            if key_name.startswith(param_name_prefix):
                key_name = key_name[len(param_name_prefix):]
            if re.match("^(%s)?%s$" % (param_name_prefix, key_name), name, re.IGNORECASE) \
                    and str(source_dict[key]).lower() != 'none'.lower():
                if not (isinstance(source_dict[key], str) or isinstance(source_dict[key], unicode)):
                    return source_dict[key]
                # AMI, newStructure tag
                # if 'notAKTR' in source_dict.keys() and source_dict['notAKTR']:
                #     return source_dict[key]
                if self.ami_client.is_new_ami_tag(source_dict):
                    return source_dict[key]
                value = re.sub(' +', ' ', source_dict[key])
                if name.find('config') > 0 and value.find('+') > 0:
                    value = ','.join(value.split('+'))
                if name.find('config') > 0 and value.find(' ') > 0:
                    value = ','.join(value.split(' '))
                if name.find('include') > 0 and value.find(' ') > 0:
                    value = ','.join(value.split(' '))
                if name.find('release') == 0 and value.find(' ') > 0:
                    value = ','.join(value.split(' '))
                if name.find('d3pdval') == 0 and value.find(' ') > 0:
                    value = ','.join(value.split(' '))
                if name.find('trigfilterlist') == 0 and value.find(' ') > 0:
                    value = ','.join(value.split(' '))
                if name == '--hepevttrigger' and value.find(' ') > 0:
                    value = ','.join(value.split(' '))
                if name.find('exec') > 0:
                    value = value.replace('%3B', ';').replace(' ;', ';').replace('; ', ';').replace('%2C', ',').replace(
                        '%2B', '+')
                    # support for AMI escaping
                    value = value.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
                    if value.find('from') == 0:
                        value_parts = value.split(' ')
                        value = ' '.join(value_parts[:4])
                        if len(value_parts) > 4:
                            value += ",%s" % ','.join(value_parts[4:])
                    elif value.find(' ') > 0:
                        value = ','.join(value.split(' '))
                return self._normalize_parameter_value(name, value, sub_steps)
        return ''

    def _get_latest_db_release(self):
        data_dict = self.rucio_client.get_datasets_and_containers(TaskDefConstants.DEFAULT_DB_RELEASE_DATASET_NAME_BASE)
        dataset_list = sorted(data_dict['datasets'], reverse=True)
        return dataset_list[0]

    def _get_parameter_name(self, name, source_dict):
        param_name = None
        name = name.lower()
        for key in source_dict.keys():
            if re.match("^(--)?%s.*$" % key, name, re.IGNORECASE):
                param_name = key
                break
        if param_name:
            return param_name
        else:
            raise Exception("%s parameter is not found" % name)

    def _get_input_output_param_name(self, params, input_type, extended_pattern=False):
        if extended_pattern:
            pattern = r'^(--)?(input|output).*%s.*File$' % input_type
        else:
            pattern = r'^(--)?(input|output)%s.*File$' % input_type
        for key in params.keys():
            if re.match(pattern, key, re.IGNORECASE):
                return key
        return None

    def _get_output_params_order(self, task_proto_dict):
        order_dict = dict()
        count = 0
        for param in task_proto_dict['job_params']:
            if 'param_type' in param.keys():
                if param['param_type'] == 'output':
                    order_dict.update({self.parse_data_name(param['dataset'])['data_type']: count})
                    count += 1
        return order_dict

    def _get_primary_input(self, job_parameters):
        for job_param in job_parameters:
            if not 'param_type' in job_param.keys() or job_param['param_type'].lower() != 'input'.lower():
                continue
            if re.match(r'^(--)?input.*File', job_param['value'], re.IGNORECASE):
                result = re.match(r'^(--)?input(?P<intype>.*)File', job_param['value'], re.IGNORECASE)
                if not result:
                    continue
                in_type = result.groupdict()['intype']
                # FIXME: replace to the method of Protocol
                if in_type.lower() == 'logs'.lower() or re.match(r'^(Low|High)PtMinbias.*$', in_type, re.IGNORECASE):
                    continue
                return job_param
        return None

    def _get_job_parameter(self, name, job_parameters):
        for job_param in job_parameters:
            if re.match(r"^(--)?%s" % name, job_param['value'], re.IGNORECASE):
                return job_param
        return None

    def _check_number_of_events(self, step, project_mode):
        if step.request.request_type.lower() == 'MC'.lower():
            number_of_events = int(step.input_events)
            project = self._get_project(step)
            bunchspacing = None
            if 'bunchspacing'.lower() in project_mode.keys():
                bunchspacing = str(project_mode['bunchspacing'.lower()])
            campaign = ':'.join(filter(None, (step.request.campaign, step.request.subcampaign, bunchspacing,)))
            if number_of_events > 0:
                small_events_numbers = dict()
                small_events_numbers.update({
                    r'mc15_13TeV': 10000,
                    r'mc16_13TeV': 2000,
                    r'mc15:mc15(a|b|c)': 10000,
                    r'mc16:mc16(a|b|c|\*)': 2000
                })
                small_events_threshold = 0
                for pattern in small_events_numbers.keys():
                    if re.match(pattern, project, re.IGNORECASE) or re.match(pattern, campaign, re.IGNORECASE):
                        small_events_threshold = small_events_numbers[pattern]
                        break
                if number_of_events < small_events_threshold:
                    force_small_events = False
                    if 'isSmallEvents'.lower() in project_mode.keys():
                        option_value = str(project_mode['isSmallEvents'.lower()])
                        if option_value.lower() == 'yes'.lower():
                            force_small_events = True
                    if not force_small_events:
                        raise TaskSmallEventsException(number_of_events)

    def _enum_previous_tasks(self, task_id, data_type, list_task_id):
        task = TTask.objects.get(id=task_id)
        task_params = json.loads(task.jedi_task_param)['jobParameters']
        primary_input = self._get_primary_input(task_params)
        if primary_input:
            dsn = primary_input['dataset']
            if re.match(TaskDefConstants.DEFAULT_DATA_NAME_PATTERN, dsn):
                parent_id = self._get_parent_task_id_from_input(dsn)
                dsn_dict = self.parse_data_name(dsn)
                if parent_id and dsn_dict['data_type'] == data_type:
                    list_task_id.append(parent_id)
                    self._enum_previous_tasks(parent_id, data_type, list_task_id)

    def _enum_next_tasks(self, task_id, data_type, list_task_id):
        next_task_list = ProductionTask.objects.filter(primary_input__endswith='_tid{0}_00'.format(task_id),
                                                       output_formats__contains=data_type)
        for next_task in next_task_list:
            list_task_id.append(int(next_task.id))
            self._enum_next_tasks(int(next_task.id), data_type, list_task_id)

    def _extract_chain_input_events(self, step):
        if step.step_parent_id == step.id:
            return step.input_events

        parent_step = StepExecution.objects.get(id=step.step_parent_id)

        if parent_step.status.lower() != self.protocol.STEP_STATUS[StepStatus.APPROVED].lower():
            return step.input_events

        if parent_step.input_events != -1:
            return parent_step.input_events

        return self._extract_chain_input_events(parent_step)

    def _check_task_number_of_jobs(self, task, nevents, step):
        number_of_jobs = 0
        nevents_per_job = task.get('nEventsPerJob', 0)

        primary_input = self._get_primary_input(task['jobParameters'])
        if not primary_input:
            return

        dsn = primary_input['dataset']
        if not dsn:
            return

        if nevents > 0:
            number_of_jobs = nevents / nevents_per_job
        else:
            total_nevents = 0
            try:
                total_nevents = self.rucio_client.get_number_events(dsn)
                if not total_nevents:
                    raise EmptyDataset()
            except:
                chain_input_events = self._extract_chain_input_events(step)
                if chain_input_events > 0:
                    total_nevents = chain_input_events
            number_of_jobs = total_nevents / nevents_per_job

        if number_of_jobs >= TaskDefConstants.DEFAULT_MAX_NUMBER_OF_JOBS_PER_TASK / 5 or \
                number_of_jobs < TaskDefConstants.NO_ES_MIN_NUMBER_OF_EVENTS / nevents_per_job:
            task['esConvertible'] = False

        if number_of_jobs >= TaskDefConstants.NO_ES_MIN_NUMBER_OF_EVENTS / nevents_per_job:
            task['skipShortInput'] = True

    def _check_task_merged_input(self, task, step, prod_step):
        # skip EI tasks
        if step.request.request_type.lower() == 'EVENTINDEX'.lower():
            return

        # skip evgen/merging/super-merging
        if prod_step.lower() == 'merge'.lower() or prod_step.lower() == 'evgen'.lower():
            return

        primary_input = self._get_primary_input(task['jobParameters'])
        if not primary_input:
            return

        dsn = primary_input['dataset']
        if not dsn:
            return

        dsn_dict = self.parse_data_name(dsn)

        if dsn_dict['prod_step'].lower() == 'merge'.lower():
            return

        task_id = self._get_parent_task_id_from_input(dsn)
        if task_id == 0:
            return

        next_tasks = list()
        self._enum_next_tasks(task_id, dsn_dict['data_type'], next_tasks)
        if not next_tasks:
            return

        for next_task_id in next_tasks:
            task_list = ProductionTask.objects.filter(project=step.request.project,
                                                      ctag=step.step_template.ctag,
                                                      primary_input__endswith='_tid{0}_00'.format(next_task_id))
            for prod_task in task_list:
                if prod_task.status in ['failed', 'broken', 'aborted', 'obsolete', 'toabort']:
                    continue

                requested_output_types = step.step_template.output_formats.split('.')
                previous_output_types = prod_task.output_formats
                processed_output_types = [e for e in requested_output_types if e in previous_output_types]
                if not processed_output_types:
                    continue

                raise MergedInputProcessedException(prod_task.id)

    def _check_task_unmerged_input(self, task, step, prod_step):
        # skip EI tasks
        if step.request.request_type.lower() == 'EVENTINDEX'.lower():
            return

        # skip evgen/merging/super-merging
        if prod_step.lower() == 'merge'.lower() or prod_step.lower() == 'evgen'.lower():
            return

        primary_input = self._get_primary_input(task['jobParameters'])
        if not primary_input:
            return

        dsn = primary_input['dataset']
        if not dsn:
            return

        dsn_dict = self.parse_data_name(dsn)

        if dsn_dict['prod_step'].lower() != 'merge'.lower():
            return

        merge_task_id = self._get_parent_task_id_from_input(dsn)
        if merge_task_id == 0:
            return

        previous_tasks = list()
        self._enum_previous_tasks(merge_task_id, dsn_dict['data_type'], previous_tasks)
        if not previous_tasks:
            return

        for previous_task_id in previous_tasks:
            task_list = ProductionTask.objects.filter(project=step.request.project,
                                                      ctag=step.step_template.ctag,
                                                      primary_input__endswith='_tid{0}_00'.format(previous_task_id))
            for prod_task in task_list:
                if prod_task.status in ['failed', 'broken', 'aborted', 'obsolete', 'toabort']:
                    continue

                requested_output_types = step.step_template.output_formats.split('.')
                previous_output_types = prod_task.output_formats
                processed_output_types = [e for e in requested_output_types if e in previous_output_types]
                if not processed_output_types:
                    continue

                raise UnmergedInputProcessedException(prod_task.id)

    def _check_task_cache_version_consistency(self, task, step, prod_step, trf_release):
        if step.request.request_type.lower() != 'GROUP'.lower():
            return

        primary_input = self._get_primary_input(task['jobParameters'])
        if not primary_input:
            return

        dsn = primary_input['dataset']
        if not dsn:
            return

        dsn_dict = self.parse_data_name(dsn)

        if dsn_dict['data_type'].lower() != 'AOD'.lower():
            return

        parent_task_id = self._get_parent_task_id_from_input(dsn)
        if parent_task_id == 0:
            return

        previous_tasks = list()
        previous_tasks.append(parent_task_id)
        self._enum_previous_tasks(parent_task_id, dsn_dict['data_type'], previous_tasks)
        if not previous_tasks:
            return

        for previous_task_id in previous_tasks:
            previous_task = ProductionTask.objects.get(id=previous_task_id)
            previous_task_ctag = self._get_ami_tag_cached(previous_task.ctag)
            previous_task_prod_step = self._get_prod_step(previous_task.ctag, previous_task_ctag)
            if previous_task_prod_step.lower() == 'merge'.lower():
                continue
            previous_task_trf_release = previous_task_ctag['SWReleaseCache'].split('_')[1]
            if int(trf_release.split('.')[0]) != int(previous_task_trf_release.split('.')[0]):
                raise WrongCacheVersionUsedException(trf_release, previous_task_trf_release)

    def _check_task_input(self, task, task_id, number_of_events, task_config, parent_task_id, input_data_name, step,
                          primary_input_offset=0, prod_step=None, reuse_input=None, evgen_params=None,
                          task_common_offset=None):
        primary_input = self._get_primary_input(task['jobParameters'])
        if not primary_input:
            logger.info("Task Id = %d, No primary input. Checking of input is skipped" % task_id)
            return

        if prod_step.lower() == 'merge'.lower():
            dsn = primary_input['dataset']
            tag_name = step.step_template.ctag
            version = self.parse_data_name(dsn)['version']
            if str(version.split('_tid')[0]).endswith(tag_name):
                merge_nevents_per_job = task.get('nEventsPerJob', 0)
                merge_nevents_per_input_file = task.get('nEventsPerInputFile', 0)
                merge_nfiles_per_job = task.get('nFilesPerJob', 0)
                merge_ngb_per_job = task.get('nGBPerJob', 0)
                is_merge_1_to_1 = True
                if merge_nevents_per_job > 0 and merge_nevents_per_input_file > 0:
                    if merge_nevents_per_job != merge_nevents_per_input_file:
                        is_merge_1_to_1 = False
                elif merge_nfiles_per_job > 1:
                    is_merge_1_to_1 = False
                elif merge_ngb_per_job > 0:
                    is_merge_1_to_1 = False
                if is_merge_1_to_1:
                    raise InvalidMergeException(dsn, tag_name)

        lost_files_exception = None

        try:
            prod_dataset = ProductionDataset.objects.filter(
                name__endswith=primary_input['dataset'].split(':')[-1]).first()
            if prod_dataset:
                prod_task = ProductionTask.objects.filter(id=prod_dataset.task_id).first()
                if prod_task:
                    if prod_task.status in ['failed', 'broken', 'aborted', 'obsolete', 'toabort']:
                        raise ParentTaskInvalid(prod_task.id, prod_task.status)
                if prod_dataset.ddm_status and prod_dataset.ddm_status == TaskDefConstants.DDM_LOST_STATUS:
                    raise InputLostFiles(prod_dataset.name)
        except InputLostFiles as ex:
            lost_files_exception = ex

        try:
            if lost_files_exception:
                if step.request.reference:
                    jira_client = JIRAClient()
                    jira_client.authorize()
                    jira_client.log_exception(step.request.reference, lost_files_exception)
        except Exception as ex:
            logger.exception('Exception occurred: {0}'.format(ex))

        if not 'nEventsPerInputFile' in task_config.keys():
            nevents_per_files = self.get_events_per_file(primary_input['dataset'])
            if not nevents_per_files:
                logger.info(
                    "Step = {0}, nEventsPerInputFile is unavailable (dataset = {1})".format(step.id, input_data_name))
            task_config['nEventsPerInputFile'] = nevents_per_files
            log_msg = "_check_task_input, step = %d, input_data_name = %s, found nEventsPerInputFile = %d" % \
                      (step.id, input_data_name, task_config['nEventsPerInputFile'])
            logger.info(log_msg)

        primary_input_total_files = 0

        try:
            primary_input_total_files = self.rucio_client.get_number_files(primary_input['dataset'])
        except:
            logger.info('_check_task_input, get_number_files for {0} failed (parent_task_id = {2}): {1}'.format(
                primary_input['dataset'], get_exception_string(), parent_task_id))
            # FIXME: move input name to protocol
            task_output_name_suffix = '_tid{0}_00'.format(TaskDefConstants.DEFAULT_TASK_ID_FORMAT % parent_task_id)
            if not str(primary_input['dataset']).endswith(task_output_name_suffix):
                raise NumberOfFilesUnavailable(primary_input['dataset'], get_exception_string())

        logger.info("primary_input_total_files={0} ({1})".format(primary_input_total_files, primary_input['dataset']))

        if primary_input_total_files > 0:
            self.verify_data_uniform(step, primary_input['dataset'])

        number_of_jobs = 0
        if number_of_events > 0 and 'nEventsPerJob' in task_config.keys():
            number_of_jobs = number_of_events / int(task_config['nEventsPerJob'])
        elif 'nEventsPerInputFile' in task_config.keys() and 'nEventsPerJob' in task_config.keys() \
                and primary_input_total_files > 0:
            number_of_jobs = \
                primary_input_total_files * int(task_config['nEventsPerInputFile']) / int(task_config['nEventsPerJob'])

        if number_of_jobs > TaskDefConstants.DEFAULT_MAX_NUMBER_OF_JOBS_PER_TASK:
            raise MaxJobsPerTaskLimitExceededException(number_of_jobs)

        if task_common_offset:
            task_common_offset_hashtag = TaskDefConstants.DEFAULT_TASK_COMMON_OFFSET_HASHTAG_FORMAT.format(
                task_common_offset
            )
            try:
                hashtag = HashTag.objects.get(hashtag=task_common_offset_hashtag)
            except ObjectDoesNotExist:
                hashtag = HashTag(hashtag=task_common_offset_hashtag, type='UD')
                hashtag.save()
            dsn_no_scope = primary_input['dataset'].split(':')[-1]
            for task_same_hashtag in ProductionTask.get_tasks_by_hashtag(hashtag.hashtag):
                if task_same_hashtag.status in ['failed', 'broken', 'aborted', 'obsolete', 'toabort']:
                    continue
                task_params = json.loads(TTask.objects.get(id=task_same_hashtag.id).jedi_task_param)
                task_input = self._get_primary_input(task_params['jobParameters'])
                task_dsn_no_scope = task_input['dataset'].split(':')[-1]
                if task_dsn_no_scope == dsn_no_scope:
                    current_offset = int(task_input['offset']) + int(task_params['nFiles'])
                    primary_input_offset = current_offset
                    break

        number_of_input_files_used = 0
        previous_tasks = list()

        # search existing task with same input_data_name and tag in ProdSys1 and ProdSys2
        ps1_task_list = TTaskRequest.objects.filter(~Q(status__in=['failed', 'broken', 'aborted', 'obsolete']),
                                                    project=step.request.project,
                                                    inputdataset=input_data_name,
                                                    ctag=step.step_template.ctag,
                                                    formats=step.step_template.output_formats)
        for ps1_task in ps1_task_list:
            previous_tasks.append(int(ps1_task.reqid))
            number_of_input_files_used += \
                int(((
                             ps1_task.total_events / ps1_task.events_per_file) / ps1_task.total_req_jobs or 0) * ps1_task.total_req_jobs or 0)

        task_list = ProductionTask.objects.filter(~Q(status__in=['failed', 'broken', 'aborted', 'obsolete', 'toabort']),
                                                  project=step.request.project,
                                                  # inputdataset=input_data_name,
                                                  step__step_template__ctag=step.step_template.ctag).filter(
            Q(inputdataset=input_data_name) |
            # Q(inputdataset__endswith=input_data_name.split(':')[-1]) |
            Q(inputdataset__contains=input_data_name.split('/')[0].split(':')[-1]) |
            Q(step__slice__input_dataset=input_data_name) |
            # Q(step__slice__input_dataset__endswith=input_data_name.split(':')[-1]) |
            Q(step__slice__input_dataset__contains=input_data_name.split('/')[0].split(':')[-1]) |
            Q(step__slice__input_data=input_data_name) |
            # Q(step__slice__input_data__endswith=input_data_name.split(':')[-1])
            Q(step__slice__input_data__contains=input_data_name.split('/')[0].split(':')[-1])
        )

        for prod_task_existing in task_list:

            # comparing output formats
            requested_output_types = step.step_template.output_formats.split('.')
            previous_output_types = prod_task_existing.step.step_template.output_formats.split('.')
            processed_output_types = [e for e in requested_output_types if e in previous_output_types]
            if not processed_output_types:
                continue

            # FIXME: support for _Cont% (ContF, ..., ContJfinal)
            container_name = "%s_Cont" % input_data_name.split('/')[0].split(':')[-1]
            if container_name in prod_task_existing.step.slice.input_dataset:
                continue

            task_id = int(prod_task_existing.id)
            previous_tasks.append(task_id)
            jedi_task_existing = TTask.objects.get(id=prod_task_existing.id)
            task_existing = json.loads(jedi_task_existing.jedi_task_param)

            if 'use_real_nevents' in task_existing.keys():
                raise Exception('Extensions are not allowed if useRealNumEvents is specified')

            # if prod_step.lower() == 'merge'.lower():
            previous_dsn = self._get_primary_input(task_existing['jobParameters'])['dataset']
            previous_dsn_no_scope = previous_dsn.split(':')[-1]
            # result = self.ddm_wrapper.get_datasets_and_containers(input_data_name, datasets_contained_only=True)
            # datasets_no_scope = [e.split(':')[-1] for e in result['datasets']]
            # if not previous_dsn_no_scope in datasets_no_scope:
            #     continue
            current_dsn = primary_input['dataset']
            current_dsn_no_scope = current_dsn.split(':')[-1]

            is_current_dsn_tid_type = bool(re.match(r'^.+_tid(?P<tid>\d+)_00$', current_dsn_no_scope, re.IGNORECASE))
            is_previous_dsn_tid_type = bool(re.match(r'^.+_tid(?P<tid>\d+)_00$', previous_dsn_no_scope, re.IGNORECASE))

            if is_current_dsn_tid_type != is_previous_dsn_tid_type:
                if not is_current_dsn_tid_type:
                    raise Exception('Mixed input for tasks with the same configuration is not allowed. ' +
                                    'Current input is {0}, the previous task ({1}) used {2} as input'.format(
                                        current_dsn_no_scope, task_id, previous_dsn_no_scope
                                    ))

            if current_dsn_no_scope != previous_dsn_no_scope:
                continue

            if prod_task_existing.status == 'done':
                if 'nFiles' in task_existing:
                    number_of_input_files_used += int(task_existing['nFiles'])
                else:
                    if 'nEventsPerJob' in task_existing and 'nEventsPerInputFile' in task_existing:
                        try:
                            jedi_dataset_info = JediDataset.objects.get(task_id=jedi_task_existing.id,
                                                                        dataset_name__contains=primary_input['dataset'])
                            number_files_finished = int(jedi_dataset_info.number_files_finished)
                            number_of_input_files = \
                                int(float(task_existing['nEventsPerJob']) / float(
                                    task_existing['nEventsPerInputFile']) * number_files_finished)
                            number_of_input_files_used += int(number_of_input_files)
                        except ObjectDoesNotExist:
                            current_dsn = primary_input['dataset']
                            previous_dsn = self._get_primary_input(task_existing['jobParameters'])['dataset']
                            if current_dsn == previous_dsn:
                                raise Exception('Task duplication candidate is found: task_id={0}. '.format(task_id) +
                                                '(The part of) input was already processed')
                    else:
                        raise TaskDuplicateDetected(task_id, 1,
                                                    request=step.request.id,
                                                    slice=step.slice.slice,
                                                    processed_formats='.'.join(processed_output_types),
                                                    requested_formats='.'.join(requested_output_types),
                                                    tag=step.step_template.ctag)
            elif prod_task_existing.status == 'finished':
                if 'nFiles' in task_existing:
                    number_of_input_files_used += int(task_existing['nFiles'])
                else:
                    if 'nEventsPerJob' in task_existing and 'nEventsPerInputFile' in task_existing:
                        try:
                            jedi_dataset_info = JediDataset.objects.get(task_id=jedi_task_existing.id,
                                                                        dataset_name__contains=primary_input['dataset'])
                            nfiles = int(jedi_dataset_info.nfiles)
                            number_of_input_files = \
                                int(float(task_existing['nEventsPerJob']) / float(
                                    task_existing['nEventsPerInputFile']) * nfiles)
                            number_of_input_files_used += int(number_of_input_files)
                        except ObjectDoesNotExist:
                            current_dsn = primary_input['dataset']
                            previous_dsn = self._get_primary_input(task_existing['jobParameters'])['dataset']
                            if current_dsn == previous_dsn:
                                raise Exception('Task duplication candidate is found: task_id={0}. '.format(task_id) +
                                                '(The part of) input was already processed')
                    else:
                        raise TaskDuplicateDetected(task_id, 3,
                                                    request=step.request.id,
                                                    slice=step.slice.slice,
                                                    processed_formats='.'.join(processed_output_types),
                                                    requested_formats='.'.join(requested_output_types),
                                                    tag=step.step_template.ctag)
            else:
                if 'nFiles' in task_existing:
                    number_of_input_files_used += int(task_existing['nFiles'])
                else:
                    raise TaskDuplicateDetected(task_id, 2,
                                                request=step.request.id,
                                                slice=step.slice.slice,
                                                processed_formats='.'.join(processed_output_types),
                                                requested_formats='.'.join(requested_output_types),
                                                tag=step.step_template.ctag)
            log_msg = "[NotERROR][Check duplicates] request=%d, chain=%d (%d), previous_task=%d (%s), n_files_used=%d" % \
                      (step.request.id, step.slice.slice, step.id, task_id, prod_task_existing.status,
                       number_of_input_files_used)
            logger.debug(log_msg)
            # if prod_task_existing.status in ['finished', 'done']:
            #     number_of_jobs = int(jedi_task_existing.total_done_jobs or 0)
            # else:
            #     number_of_jobs = int(jedi_task_existing.total_req_jobs or 0)
            # if 'nFilesPerJob' in task_existing:
            #     number_of_input_files_used += int(task_existing['nFilesPerJob']) * number_of_jobs
            # elif 'nEventsPerJob' in task_existing and 'nEventsPerInputFile' in task_existing:
            #     number_of_input_files = \
            #         int(float(task_existing['nEventsPerJob']) / float(task_existing['nEventsPerInputFile']) * number_of_jobs)
            #     # if 'mergeSpec' in task_existing or 'esmergeSpec' in task_existing:
            #     #     number_of_input_files /= 2
            #     if number_of_input_files == 0:
            #         number_of_input_files = int(prod_task_existing.step.input_events / task_existing['nEventsPerInputFile'])
            #     number_of_input_files_used += int(number_of_input_files)
            # else:
            #     logger.info("Task Id = %d, number of used files is unknown", prod_task_existing.id)

        if number_of_events > 0:
            number_input_files_requested = number_of_events / int(task_config['nEventsPerInputFile'])
        else:
            number_input_files_requested = primary_input_total_files - number_of_input_files_used

        if reuse_input:
            primary_input['offset'] = 0
            for param in task['jobParameters']:
                if 'dataset' in param.keys() and param['dataset'] == 'seq_number':
                    param['offset'] = number_of_input_files_used
            return

        if (number_input_files_requested + number_of_input_files_used) > primary_input_total_files \
                or number_input_files_requested < 0:
            if number_input_files_requested < 0:
                logger.error('[ERROR] number_input_files_requested={0}, request={1}, chain={2} ({3})'.format(
                    number_input_files_requested, step.request.id, step.slice.slice, step.id))
            raise NoMoreInputFiles("No more input files. requested/used/total = %d/%d/%d, previous_tasks = %s" %
                                   (number_input_files_requested, number_of_input_files_used, primary_input_total_files,
                                    str(previous_tasks)))
        else:
            primary_input['offset'] = number_of_input_files_used
            random_seed_param = self._get_job_parameter('randomSeed', task['jobParameters'])
            if random_seed_param:
                random_seed_param['offset'] = number_of_input_files_used
            if prod_step.lower() == 'evgen'.lower():
                events_per_file = int(task_config['nEventsPerInputFile'])
                first_event_param = self._get_job_parameter('firstEvent', task['jobParameters'])
                if first_event_param:
                    first_event_param['offset'] = number_of_input_files_used * events_per_file

        if evgen_params:
            random_seed_param = self._get_job_parameter('randomSeed', task['jobParameters'])
            if random_seed_param:
                random_seed_param['offset'] = evgen_params['offset']
            first_event_param = self._get_job_parameter('firstEvent', task['jobParameters'])
            if first_event_param:
                first_event_param['offset'] = evgen_params['event_offset']

        if primary_input_offset:
            primary_input['offset'] = primary_input_offset

    def _get_merge_tag_name(self, step):
        task_config = self._get_task_config(step)
        project_mode = self._get_project_mode(step)
        merging_tag_name = ''
        if 'merging_tag' in task_config.keys():
            merging_tag_name = task_config['merging_tag']
        if not merging_tag_name:
            if 'merging' in project_mode.keys():
                merging_tag_name = project_mode['merging']
        return merging_tag_name

    def _define_merge_params(self, step, task_proto_dict, train_production=False):
        task_config = self._get_task_config(step)

        merging_tag_name = ''

        # FIXME: use _get_merge_tag_name
        if 'merging_tag' in task_config.keys():
            merging_tag_name = task_config['merging_tag']
        if 'nFilesPerMergeJob' in task_config.keys():
            merging_number_of_files_per_job = int(task_config['nFilesPerMergeJob'])
            task_proto_dict.update({'merging_number_of_files_per_job': merging_number_of_files_per_job})
        if 'nGBPerMergeJob' in task_config.keys():
            merging_number_of_gb_pef_job = int(task_config['nGBPerMergeJob'])
            task_proto_dict.update({'merging_number_of_gb_pef_job': merging_number_of_gb_pef_job})
        if 'nMaxFilesPerMergeJob' in task_config.keys():
            merging_number_of_max_files_per_job = int(task_config['nMaxFilesPerMergeJob'])
            task_proto_dict.update({'merging_number_of_max_files_per_job': merging_number_of_max_files_per_job})

        if not merging_tag_name:
            # get project_mode without valid cmtconfig
            project_mode = self._get_project_mode(step)
            if 'merging' in project_mode.keys():
                merging_tag_name = project_mode['merging']

        if not merging_tag_name:
            logger.debug("[merging] Merging tag name is not specified")
            return

        ctag = self._get_ami_tag_cached(merging_tag_name)  # self.ami_wrapper.get_ami_tag(merging_tag_name)

        if ',' in ctag['transformation']:
            raise Exception("[merging] JEDI does not support tags with multiple transformations")

        trf_name = ctag['transformation']
        trf_cache = ctag['SWReleaseCache'].split('_')[0]
        trf_release = ctag['SWReleaseCache'].split('_')[1]
        trf_params = self.ami_client.get_trf_params(trf_cache, trf_release, trf_name, force_ami=True)

        # proto_fix
        if trf_name.lower() == 'HLTHistMerge_tf.py'.lower():
            # if 'HIST' in output_types:
            # output_types.remove('HIST')
            # output_types.append('HIST_MRG')
            if not '--inputHISTFile' in trf_params:
                trf_params.append('--inputHISTFile')
            if not '--outputHIST_MRGFile' in trf_params:
                trf_params.remove('--outputHISTFile')
                trf_params.append('--outputHIST_MRGFile')
        elif trf_name.lower() == 'DAODMerge_tf.py'.lower():
            # FIXME: use dumpArgs
            input_params = ["--input%sFile" % output_format for output_format in
                            step.step_template.output_formats.split('.')]
            for input_param in input_params:
                if not input_param in trf_params:
                    trf_params.append(input_param)
                    result = re.match(r'^(--)?input(?P<intype>.*)File', input_param, re.IGNORECASE)
                    if result:
                        in_type = result.groupdict()['intype']
                        output_param = "--output%s_MRGFile" % in_type
                        if not output_param in trf_params:
                            trf_params.append(output_param)
            if not '--inputDAOD_EGAM1File' in trf_params:
                trf_params.append('--inputDAOD_EGAM1File')
            if not '--inputDAOD_EGAM3File' in trf_params:
                trf_params.append('--inputDAOD_EGAM3File')
            if not '--outputDAOD_EGAM1_MRGFile' in trf_params:
                trf_params.append('--outputDAOD_EGAM1_MRGFile')
            if not '--outputDAOD_EGAM3_MRGFile' in trf_params:
                trf_params.append('--outputDAOD_EGAM3_MRGFile')

        trf_options = {}
        for key in Protocol.TRF_OPTIONS.keys():
            if re.match(key, trf_name, re.IGNORECASE):
                trf_options.update(Protocol.TRF_OPTIONS[key])

        input_count = 0
        output_count = 0
        merging_job_parameters = list()
        for name in trf_params:
            if re.match(r'^(--)?amiTag$', name, re.IGNORECASE):
                param_dict = {'name': name, 'value': merging_tag_name}
                param_dict.update(trf_options)
                merging_job_parameters.append(self.protocol.render_param(TaskParamName.CONSTANT, param_dict))
            elif re.match(r'^(--)?input.*File$', name, re.IGNORECASE):
                result = re.match(r'^(--)?input(?P<intype>.*)File$', name, re.IGNORECASE)
                if not result:
                    continue
                merging_input_type = result.groupdict()['intype']
                if merging_input_type.lower() == 'Logs'.lower():
                    logs_param_dict = {'name': name, 'value': "${TRN_LOG0}"}
                    logs_param_dict.update(trf_options)
                    merging_job_parameters.append(self.protocol.render_param(TaskParamName.CONSTANT, logs_param_dict))
                    continue
                order_dict = self._get_output_params_order(task_proto_dict)
                for output_type in order_dict:
                    output_internal_type = output_type.split('_')[0]
                    if (merging_input_type in output_internal_type) or (
                            output_type == merging_input_type):  # or (output_internal_type in merging_input_type):
                        if step.request.request_type.lower() in ['GROUP'.lower()]:
                            param_dict = {'name': name, 'value': "${TRN_OUTPUT%d/L}" % order_dict[output_type]}
                        else:
                            param_dict = {'name': name, 'value': "${TRN_OUTPUT%d}" % order_dict[output_type]}
                        param_dict.update(trf_options)
                        merging_job_parameters.append(self.protocol.render_param(TaskParamName.CONSTANT, param_dict))
                        input_count += 1
                        break
            elif re.match(r'^(--)?output.*File$', name, re.IGNORECASE):
                result = re.match(r'^(--)?output(?P<intype>.*)File$', name, re.IGNORECASE)
                if not result:
                    continue
                merging_output_type = result.groupdict()['intype']
                merging_output_internal_type = ''
                if re.match(r'^(--)?output.*_MRGFile$', name, re.IGNORECASE):
                    result = re.match(r'^(--)?output(?P<type>\w+)_MRGFile$', name, re.IGNORECASE)
                    if result:
                        merging_output_internal_type = result.groupdict()['type']
                order_dict = self._get_output_params_order(task_proto_dict)
                for output_type in order_dict:
                    output_internal_type = output_type.split('_')[0]
                    if (merging_output_type in output_internal_type) or (output_type == merging_output_type) or \
                            (merging_output_internal_type and merging_output_internal_type in output_internal_type) or \
                            (output_type == merging_output_internal_type):
                        param_dict = {'name': name, 'value': "${OUTPUT%d}" % order_dict[output_type]}
                        param_dict.update(trf_options)
                        merging_job_parameters.append(self.protocol.render_param(TaskParamName.CONSTANT, param_dict))
                        output_count += 1
                        break
            else:
                param_value = self._get_parameter_value(name, ctag)
                if not param_value or str(param_value).lower() == 'none':
                    continue
                if str(param_value).lower() == 'none,none':
                    continue
                param_dict = {'name': name, 'value': param_value}
                param_dict.update(trf_options)

                if re.match('^(--)?autoConfiguration', name, re.IGNORECASE):
                    if ' ' in param_value:
                        param_dict.update({'separator': ' '})

                merging_job_parameters.append(self.protocol.render_param(TaskParamName.CONSTANT, param_dict))

        if input_count == 0:
            raise Exception('[merging] no inputs')

        if output_count == 0:
            raise Exception('[merging] no outputs')

        merging_job_parameters_str = ' '.join([param['value'] for param in merging_job_parameters])

        if train_production:
            task_proto_dict['um_name_at_end'] = True
        task_proto_dict['merge_output'] = True
        task_proto_dict['merge_spec'] = dict()
        task_proto_dict['merge_spec']['transPath'] = trf_name
        task_proto_dict['merge_spec']['jobParameters'] = merging_job_parameters_str

    def _get_prod_step(self, ctag_name, ctag):
        prod_step = str(ctag['productionStep']).replace(' ', '')
        if ctag_name[0] in ('a',):
            if 'Reco'.lower() in ctag['transformation'].lower():
                prod_step = 'recon'
            elif 'Sim'.lower() in ctag['transformation'].lower():
                prod_step = 'simul'
        return prod_step

    # FIXME: move to datamgmt
    def _get_ami_tag_cached(self, tag_name):
        try:
            ctag = self._tag_cache.get(tag_name)
        except AttributeError:
            self._tag_cache = dict()
            ctag = None
        if not ctag:
            ctag = self.ami_client.get_ami_tag(tag_name)
            self._tag_cache.update({tag_name: ctag})
        return ctag

    def _check_task_events_consistency(self, task_config):
        n_events_input_file = int(task_config['nEventsPerInputFile'])
        n_events_job = int(task_config['nEventsPerJob'])
        if (n_events_input_file % n_events_job == 0) or (n_events_job % n_events_input_file == 0):
            pass
        else:
            raise Exception(
                "The task is rejected because of inconsistency. " +
                "nEventsPerInputFile=%d, nEventsPerJob=%d" % (n_events_input_file, n_events_job)
            )

    def create_task_chain(self, step_id, max_number_of_steps=None, restart=None, input_dataset=None,
                          first_step_number_of_events=0, primary_input_offset=0, first_parent_task_id=0,
                          container_name=None, evgen_params=None):
        logger.info("Processing step %d" % step_id)

        try:
            first_step = StepExecution.objects.get(id=step_id)
            if first_step_number_of_events:
                first_step.input_events = int(first_step_number_of_events)
        except ObjectDoesNotExist:
            raise Exception("Step %d is not found" % step_id)

        chain = list()
        chain.append(first_step)

        step = first_step
        chain_id = 0
        if first_parent_task_id:
            parent_task_id = first_parent_task_id
        else:
            parent_task_id = self.task_reg.get_parent_task_id(step, 0)

        while step is not None:
            try:
                step = StepExecution.objects.get(~Q(id=step.id), step_parent_id=step.id, slice=first_step.slice)
                if step.status.lower() == self.protocol.STEP_STATUS[StepStatus.APPROVED].lower():
                    chain.append(step)
            except ObjectDoesNotExist:
                step = None

        if max_number_of_steps:
            chain = chain[:max_number_of_steps]

        for step in chain:
            priority = step.priority
            number_of_events = int(step.input_events)
            username = step.request.manager
            usergroup = self._get_usergroup(step)
            ctag_name = step.step_template.ctag
            output_types = step.step_template.output_formats.split('.')
            project = self._get_project(step)
            ctag = self._get_ami_tag_cached(ctag_name)
            energy_gev = self._get_energy(step, ctag)
            prod_step = self._get_prod_step(ctag_name, ctag)
            task_config = self._get_task_config(step)
            memory = TaskDefConstants.DEFAULT_MEMORY
            base_memory = TaskDefConstants.DEFAULT_MEMORY_BASE

            trf_name = ctag['transformation']

            trf_options = {}
            for key in Protocol.TRF_OPTIONS.keys():
                if re.match(key, trf_name, re.IGNORECASE):
                    trf_options.update(Protocol.TRF_OPTIONS[key])

            # FIXME: tzero tags support
            if ctag_name[0] in ('f', 'm', 'v', 'k'):
                tzero_tag = self.ami_client.get_ami_tag_tzero(ctag_name)
                if type(tzero_tag) == unicode:
                    tzero_tag = json.loads(tzero_tag)
                tzero_outputs = tzero_tag['transformation']['args']['outputs']
                if type(tzero_outputs) == unicode:
                    tzero_outputs = ast.literal_eval(tzero_outputs)
                # FIXME: implement updating of step_template
                # output_types = [tzero_outputs[key]['dstype'] for key in tzero_outputs.keys()]
                try:
                    self.ami_client.apply_phconfig_ami_tag(ctag)
                except SyntaxError:
                    raise Exception('phconfig content of {0} tag has invalid syntax'.format(ctag_name))
                except Exception as ex:
                    raise Exception('apply_phconfig_ami_tag failed: {0}'.format(str(ex)))
                trf_options.update({'separator': ' '})

            if ',' in trf_name:
                raise Exception("JEDI does not support tags with multiple transformations")

            trf_cache = ctag['SWReleaseCache'].split('_')[0]
            trf_release = ctag['SWReleaseCache'].split('_')[1]
            trf_release_base = '.'.join(trf_release.split('.')[:3])

            use_nightly_release = False
            version_base = None
            version_major = None
            version_timestamp = None
            if 'T' in trf_release:
                # release contains timestamp - nightly
                # 21.1.2-21.0-2017-04-03T2135
                version_base = trf_release[:trf_release.index('-')]
                release_part = trf_release[trf_release.index('-') + 1:]
                version_major = release_part[:release_part.index('-')]
                version_timestamp = release_part[release_part.index('-') + 1:]
                use_nightly_release = True

            project_mode = self._get_project_mode(step,
                                                  "%s-%s" % (trf_cache, trf_release),
                                                  use_nightly_release=use_nightly_release)

            change_output_type_dict = dict()
            try:
                if 'changeType'.lower() in project_mode.keys():
                    for pair in str(project_mode['changeType'.lower()]).split(','):
                        change_output_type_dict[pair.split(':')[0]] = pair.split(':')[-1]
            except Exception as ex:
                raise Exception('changeType has invalid format: {0}'.format(str(ex)))

            index_consistent_param_list = list()
            try:
                if 'indexConsistent'.lower() in project_mode.keys():
                    for name in str(project_mode['indexConsistent'.lower()]).split(','):
                        index_consistent_param_list.append(name)
            except Exception as ex:
                raise Exception('indexConsistent has invalid format: {0}'.format(str(ex)))

            skip_prod_step_check = False
            if 'skipProdStepCheck'.lower() in project_mode.keys():
                option_value = str(project_mode['skipProdStepCheck'.lower()])
                if option_value.lower() == 'yes'.lower():
                    skip_prod_step_check = True
                elif option_value.lower() == 'no'.lower():
                    skip_prod_step_check = False

            if 'merge'.lower() in step.step_template.step.lower() and not 'merge' in prod_step.lower():
                if not skip_prod_step_check:
                    raise Exception('productionStep in the tag ({0}) differs from the step name in the request ({1})'
                                    .format(prod_step, step.step_template.step))

            ami_types = None
            try:
                ami_types = self.ami_client.get_types()
            except Exception as ex:
                logger.exception('Getting AMI types failed: {0}'.format(str(ex)))

            if ami_types:
                # FIXME: support for EventIndex pseudo format
                ami_types.append('EI')
                # FIXME: OverlayTest
                ami_types.append('BS_TRIGSKIM')
                for output_type in output_types:
                    if not output_type in ami_types:
                        raise Exception("The output format \"%s\" is not registered in AMI" % output_type)
            else:
                logger.warning('AMI type list is empty')

            trf_dict = dict()
            trf_dict.update({trf_name: [trf_cache, trf_release, trf_release_base]})

            force_ami = self.ami_client.is_new_ami_tag(ctag)  # False

            trf_params = list()
            trf_sub_steps = list()
            for key in trf_dict.keys():
                trf_params.extend(self.ami_client.get_trf_params(trf_dict[key][0], trf_dict[key][1], key,
                                                                 sub_step_list=trf_sub_steps, force_ami=force_ami))

            if not trf_params:
                raise Exception("AMI: list of transformation parameters is empty")

            skip_evgen_check = False
            if 'skipEvgenCheck'.lower() in project_mode.keys():
                option_value = str(project_mode['skipEvgenCheck'.lower()])
                if option_value.lower() == 'yes'.lower():
                    skip_evgen_check = True
                elif option_value.lower() == 'no'.lower():
                    skip_evgen_check = False

            use_real_nevents = None
            if 'useRealNumEvents'.lower() in project_mode.keys():
                option_value = str(project_mode['useRealNumEvents'.lower()])
                if option_value.lower() == 'yes'.lower():
                    use_real_nevents = True
                    skip_evgen_check = True
                elif option_value.lower() == 'no'.lower():
                    use_real_nevents = None

            use_containers = False
            if step.request.request_type.lower() == 'MC'.lower():
                if prod_step.lower() == 'evgen'.lower() or prod_step.lower() == 'simul'.lower():
                    use_containers = True
                if prod_step.lower() == 'simul'.lower() and not skip_evgen_check:
                    if step.step_parent_id != step.id:
                        try:
                            evgen_step = StepExecution.objects.get(id=step.step_parent_id)
                            if evgen_step.step_template.step.lower() == 'Evgen Merge'.lower():
                                raise Exception('The parent is EVNT merging. The checking is skipped')
                            if evgen_step.status != self.protocol.STEP_STATUS[StepStatus.APPROVED]:
                                raise Exception('The parent step is skipped. The checking is skipped')
                            # use only JO input
                            evgen_step.slice.input_dataset = evgen_step.slice.input_data
                            evgen_input_params = self.get_input_params(evgen_step, evgen_step, False, energy_gev,
                                                                       use_evgen_otf=True)
                            if 'nEventsPerJob' in evgen_input_params.keys():
                                evgen_events_per_job = int(evgen_input_params['nEventsPerJob'])
                                evgen_step_task_config = self._get_task_config(evgen_step)
                                evgen_step_task_config.update({'nEventsPerJob': evgen_events_per_job})
                                self._set_task_config(evgen_step, evgen_step_task_config)
                                task_config.update({'nEventsPerInputFile': evgen_events_per_job})
                                self._set_task_config(step, task_config)
                        except Exception as ex:
                            logger.warning("Checking the parent evgen step failed: %s" % str(ex))

            if 'nEventsPerInputFile' in task_config.keys():
                n_events_input_file = int(task_config['nEventsPerInputFile'])
                parent_step = StepExecution.objects.get(id=step.step_parent_id)
                is_parent_merge = 'Merge'.lower() in parent_step.step_template.step.lower()
                is_parent_approved = \
                    parent_step.status.lower() == self.protocol.STEP_STATUS[StepStatus.APPROVED].lower()
                if parent_step.id != step.id and not is_parent_merge and is_parent_approved:
                    parent_task_config = self._get_task_config(parent_step)
                    if 'nEventsPerJob' in parent_task_config.keys():
                        n_events_job_parent = int(parent_task_config['nEventsPerJob'])
                        if n_events_input_file != n_events_job_parent:
                            if 'nEventsPerInputFile'.lower() in project_mode.keys():
                                pass
                            else:
                                raise Exception(
                                    "The task is rejected because of inconsistency. " +
                                    "nEventsPerInputFile=%d does not match to nEventsPerJob=%d of the parent" %
                                    (n_events_input_file, n_events_job_parent)
                                )

            overlay_production = False
            train_production = False

            for key in ctag.keys():
                if re.match(r'^(--)?reductionConf$', key, re.IGNORECASE):
                    if str(ctag[key]).lower() != 'none':
                        train_production = True
                        break

            use_evgen_otf = False
            if 'isOTF'.lower() in project_mode.keys():
                option_value = str(project_mode['isOTF'.lower()])
                if option_value.lower() == 'yes'.lower():
                    use_evgen_otf = True
                elif option_value.lower() == 'no'.lower():
                    use_evgen_otf = False

            use_no_output = False
            if 'noOutput'.lower() in project_mode.keys():
                option_value = str(project_mode['noOutput'.lower()])
                if option_value.lower() == 'yes'.lower():
                    use_no_output = True
                elif option_value.lower() == 'no'.lower():
                    use_no_output = False

            leave_log = True
            if 'leaveLog'.lower() in project_mode.keys():
                option_value = str(project_mode['leaveLog'.lower()])
                if option_value.lower() == 'yes'.lower():
                    leave_log = True
                elif option_value.lower() == 'no'.lower():
                    leave_log = None

            if step.request.request_type.lower() == 'MC'.lower():
                if prod_step.lower() == 'simul'.lower():
                    leave_log = True

            if 'mergeCont'.lower() in project_mode.keys():
                option_value = str(project_mode['mergeCont'.lower()])
                if option_value.lower() == 'yes'.lower():
                    use_containers = True

            bunchspacing = None
            if 'bunchspacing'.lower() in project_mode.keys():
                bunchspacing = str(project_mode['bunchspacing'.lower()])

            max_events_forced = None
            if 'maxEvents'.lower() in project_mode.keys():
                max_events_forced = int(project_mode['maxEvents'.lower()])

            if 'fixedMaxEvents'.lower() in project_mode.keys():
                option_value = str(project_mode['fixedMaxEvents'.lower()])
                if option_value.lower() == 'yes'.lower():
                    input_data_name = step.slice.input_data
                    input_data_dict = self.parse_data_name(input_data_name)
                    params = self._get_evgen_input_files(input_data_dict, energy_gev, use_evgen_otf=use_evgen_otf)
                    if 'nEventsPerJob' in params:
                        max_events_forced = params['nEventsPerJob']
                    else:
                        raise Exception(
                            'JO file {0} does not contain evgenConfig.minevents definition. '.format(input_data_name) +
                            'fixedMaxEvents option cannot be used. The task is rejected')

            skip_events_forced = None
            if 'skipEvents'.lower() in project_mode.keys():
                skip_events_forced = int(project_mode['skipEvents'.lower()])

            if 'nEventsPerRange'.lower() in project_mode.keys():
                task_config.update({'nEventsPerRange': int(project_mode['nEventsPerRange'.lower()])})

            allow_no_output_patterns = list()
            try:
                if 'allowNoOutput'.lower() in project_mode.keys():
                    for pattern in str(project_mode['allowNoOutput'.lower()]).split(','):
                        allow_no_output_patterns.append(pattern)
            except Exception as ex:
                logger.exception("allowNoOutput has invalid format: %s" % str(ex))

            hidden_output_patterns = list()
            try:
                if 'hiddenOutput'.lower() in project_mode.keys():
                    for pattern in str(project_mode['hiddenOutput'.lower()]).split(','):
                        hidden_output_patterns.append(pattern)
            except Exception as ex:
                logger.exception("hiddenOutput has invalid format: %s" % str(ex))

            output_ratio = 0
            if 'outputRatio'.lower() in project_mode.keys():
                output_ratio = int(project_mode['outputRatio'.lower()])

            ignore_trf_params = list()
            try:
                if 'ignoreTrfParams'.lower() in project_mode.keys():
                    for param in str(project_mode['ignoreTrfParams'.lower()]).split(','):
                        ignore_trf_params.append(param)
            except Exception as ex:
                logger.exception("ignoreTrfParams has invalid format: %s" % str(ex))

            empty_trf_params = list()
            try:
                if 'emptyTrfParams'.lower() in project_mode.keys():
                    for param in str(project_mode['emptyTrfParams'.lower()]).split(','):
                        empty_trf_params.append(param)
            except Exception as ex:
                logger.exception("emptyTrfParams has invalid format: %s" % str(ex))

            use_dataset_name = None
            if 'useDatasetName'.lower() in project_mode.keys():
                option_value = str(project_mode['useDatasetName'.lower()])
                if option_value.lower() == 'yes'.lower():
                    use_dataset_name = True

            use_container_name = None
            if 'useContainerName'.lower() in project_mode.keys():
                option_value = str(project_mode['useContainerName'.lower()])
                if option_value.lower() == 'yes'.lower():
                    use_container_name = True

            use_direct_io = None
            if 'useDirectIo'.lower() in project_mode.keys():
                option_value = str(project_mode['useDirectIo'.lower()])
                if option_value.lower() == 'yes'.lower():
                    use_direct_io = True

            task_common_offset = None
            if 'commonOffset'.lower() in project_mode.keys():
                task_common_offset = project_mode['commonOffset'.lower()]

            env_params_dict = dict()
            try:
                if 'env'.lower() in project_mode.keys():
                    for pair in str(project_mode['env'.lower()]).split(','):
                        env_params_dict[pair.split(':')[0]] = pair.split(':')[-1]
            except Exception as ex:
                logger.exception("env parameter has invalid format: %s" % str(ex))

            secondary_input_offset = None
            if 'secondaryInputOffset'.lower() in project_mode.keys():
                secondary_input_offset = int(project_mode['secondaryInputOffset'.lower()])

            ei_output_filename = None
            if 'outputEIFile'.lower() in project_mode.keys():
                ei_output_filename = str(project_mode['outputEIFile'.lower()])

            if input_dataset:
                step.slice.input_dataset = input_dataset
            input_params = self.get_input_params(step, first_step, restart, energy_gev, use_containers, use_evgen_otf,
                                                 task_id=parent_task_id)
            if not input_params:
                input_params = self.get_input_params(step, first_step, True, energy_gev, use_containers, use_evgen_otf,
                                                     task_id=parent_task_id)

            if 'input_params' in task_config.keys():
                input_params.update(task_config['input_params'])

            if 'nFilesPerJob' in input_params.keys() and not 'nFilesPerJob' in task_config.keys():
                task_config.update({'nFilesPerJob': int(input_params['nFilesPerJob'])})

            if evgen_params:
                input_params.update(evgen_params)
                number_of_events = evgen_params['nevents']
                task_config['nFiles'] = evgen_params['nfiles']

            try:
                if step.request.request_type.lower() == 'MC'.lower() and 'nEventsPerInputFile' in task_config.keys():
                    real_input_events = 0
                    for key in input_params.keys():
                        if re.match(r'^(--)?input.*File$', key, re.IGNORECASE):
                            for input_name in input_params[key]:
                                result = re.match(r'^.+_tid(?P<tid>\d+)_00$', input_name)
                                if result:
                                    if parent_task_id == int(result.groupdict()['tid']):
                                        continue
                                real_input_events += int(
                                    task_config['nEventsPerInputFile']) * self.rucio_client.get_number_files(input_name)
                    if real_input_events < number_of_events:
                        real_input_difference = float(number_of_events - real_input_events) / float(
                            number_of_events) * 100
                        if real_input_difference <= TaskDefConstants.DEFAULT_ALLOWED_INPUT_EVENTS_DIFFERENCE:
                            number_of_events = real_input_events
                            step.input_events = number_of_events
                            step.slice.input_events = number_of_events
                            step.slice.save()
                            step.save()
            except:
                logger.warning('Checking real number of input events failed: {0}'.format(get_exception_string()))

            use_evnt_filter = None
            if self.protocol.is_evnt_filter_step(project_mode, task_config) and prod_step.lower() == 'evgen'.lower():
                input_types = list()
                for key in input_params.keys():
                    result = re.match(r'^(--)?input(?P<intype>.*)File', key, re.IGNORECASE)
                    if not result:
                        continue
                    in_type = result.groupdict()['intype']
                    input_types.append(in_type)
                if len(input_types) == 1 and 'EVNT' in input_types:
                    efficiency = 0
                    safety_factor = 0.1

                    efficiency = float(task_config.get('evntFilterEff', efficiency))
                    efficiency = float(project_mode.get('evntFilterEff'.lower(), efficiency))

                    safety_factor = float(task_config.get('evntSafetyFactor', safety_factor))
                    safety_factor = float(project_mode.get('evntSafetyFactor'.lower(), safety_factor))

                    input_data_name = step.slice.input_data
                    input_data_dict = self.parse_data_name(input_data_name)
                    max_events_forced = \
                        self._get_evgen_input_files(input_data_dict, energy_gev, use_evgen_otf=use_evgen_otf)[
                            'nEventsPerJob']
                    job_config = "%sJobOptions/%s" % (input_data_dict['project'], input_data_name)
                    input_params.update({'jobConfig': job_config})
                    input_params.update({'nEventsPerJob': max_events_forced})
                    if 'inputEVNTFile' in input_params.keys():
                        input_params['inputEVNT_PreFile'] = input_params['inputEVNTFile']
                    ignore_trf_params.append('inputEVNTFile')

                    min_events = max_events_forced
                    project_mode['nEventsPerInputFile'.lower()] = min_events

                    if not 'nEventsPerInputFile' in task_config.keys():
                        input_name = input_params[input_params.keys()[0]][0]
                        input_file_min_events = self.get_events_per_file(input_name)
                    else:
                        input_file_min_events = int(task_config['nEventsPerInputFile'])

                    number_files_per_job = int(
                        min_events / (efficiency * (1 - safety_factor)) / input_file_min_events) + 1
                    number_files = number_of_events * number_files_per_job / min_events
                    task_config['nFilesPerJob'] = number_files_per_job
                    task_config['nFiles'] = number_files

                    use_evnt_filter = True

            use_lhe_filter = None
            if prod_step.lower() == 'evgen'.lower():
                input_types = list()
                for key in input_params.keys():
                    result = re.match(r'^(--)?input(?P<intype>.*)File', key, re.IGNORECASE)
                    if not result:
                        continue
                    for input_name in input_params[key]:
                        try:
                            input_name_dict = self.parse_data_name(input_name)
                            if input_name_dict['prod_step'] == 'evgen':
                                input_types.append(input_name_dict['data_type'])
                        except Exception as ex:
                            logger.error('parse_data_name failed: {0} (input_name={1})'.format(ex, input_name))
                if len(input_types) == 1 and 'TXT' in input_types:
                    min_events = input_params.get('nEventsPerJob', None)
                    if min_events:
                        project_mode['nEventsPerInputFile'.lower()] = min_events

                        number_files_per_job = int(task_config.get('nFilesPerJob', 1))
                        number_files = number_of_events * number_files_per_job / min_events
                        task_config['nFiles'] = number_files

                        use_lhe_filter = True

            # proto_fix
            if trf_name.lower() == 'Trig_reco_tf.py'.lower():
                trf_options.update({'separator': ' '})
                for name in trf_params:
                    if re.match(r'^(--)?inputBS_RDOFile$', name, re.IGNORECASE) and 'RAW'.lower() in ','.join(
                            [e.lower() for e in input_params.keys()]):
                        input_param_name = self._get_input_output_param_name(input_params, 'RAW', extended_pattern=True)
                        if input_param_name:
                            use_bs_rdo = self._get_parameter_value('prodSysBSRDO', ctag)
                            logger.info("use_bs_rdo = %s" % str(use_bs_rdo))
                            if 'RAW' in output_types or use_bs_rdo:
                                input_params['inputBS_RDOFile'] = input_params[input_param_name]  # ['inputRAWFile']
                                trf_params.remove('--inputBSFile')
                            else:
                                input_params['inputBSFile'] = input_params[input_param_name]  # ['inputRAWFile']
                                if '--inputBS_RDOFile' in trf_params:
                                    trf_params.remove('--inputBS_RDOFile')
                        break
                if 'athenaopts' in ctag.keys() and ctag_name == 'r6395':
                    ctag[
                        'athenaopts'] = " -c \"import os;os.unsetenv('FRONTIER_SERVER');rerunLVL1=True\" -J TrigConf::HLTJobOptionsSvc --use-database --db-type Coral --db-server TRIGGERDBREPR --db-smkey 598 --db-hltpskey 401 --db-extra \"{'lvl1key': 82}\" "
            # FIXME
            # elif trf_name.lower() in [e.lower() for e in ['OverlayChain_tf.py']]:
            #     trf_options.update({'separator': ' '})
            #     for input_tag_key in [e for e in ctag.keys() if re.match(r'^(--)?input.*File$', e, re.IGNORECASE)]:
            #         input_name = self._get_parameter_value(input_tag_key, ctag)
            #         self._add_input_dataset_name(input_name, input_params)
            elif trf_name.lower() == 'POOLtoEI_tf.py'.lower():
                use_no_output = True
                # FIXME
                for key in input_params.keys():
                    if re.match(r'^(--)?input.*File$', key, re.IGNORECASE):
                        input_params['inputPOOLFile'] = input_params[key]
                        del input_params[key]
            elif trf_name.lower() == 'HITSMerge_tf.py'.lower():
                param_name = 'inputLogsFile'
                if not param_name in ignore_trf_params:
                    ignore_trf_params.append(param_name)
            # FIXME: OverlayTest
            elif trf_name.lower() == 'BSOverlayFilter_tf.py'.lower():
                overlay_production = True
                for key in input_params.keys():
                    for name in input_params[key][:]:
                        if re.match(r'^.+_tid(?P<tid>\d+)_00$', name, re.IGNORECASE):
                            input_params[key].remove(name)
                for key in input_params.keys():
                    if re.match(r'^(--)?input.*File$', key, re.IGNORECASE):
                        input_params['inputBSCONFIGFile'] = input_params[key]
                        del input_params[key]
            # elif trf_name.lower() == 'OverlayChain_tf.py'.lower():
            #     overlay_production = True

            input_data_name = None
            skip_check_input = False

            if step.step_parent_id == step.id:
                input_data_name = self.get_step_input_data_name(step)
            else:
                for key in input_params.keys():
                    if re.match(r'^(--)?input.*File$', key, re.IGNORECASE):
                        if len(input_params[key]):
                            input_data_name = input_params[key][0]
                            break

            if not input_data_name:
                raise Exception("Input data list is empty")

            if use_dataset_name and use_containers:
                datasets = self.rucio_client.list_datasets_in_container(input_data_name)
                if not datasets:
                    raise Exception(
                        'The container {0} is empty. Impossible to construct a task name'.format(input_data_name)
                    )
                input_data_name = datasets[0]

            if use_container_name and container_name:
                input_data_name = container_name

            input_data_dict = self.parse_data_name(input_data_name)

            if use_evnt_filter:
                input_data_name = step.slice.input_data
                input_data_dict = self.parse_data_name(input_data_name)

            if input_data_dict['project'].lower().startswith('mc') and project.lower().startswith('data'):
                raise Exception("The project 'data' is invalid for MC inputs")

            taskname = self._construct_taskname(input_data_name, project, prod_step, ctag_name)
            # FIXME
            task_proto_id = self.task_reg.register_task_id()

            if 'EVNT' in output_types and prod_step.lower() == 'evgen'.lower():
                use_evnt_txt = False
                if 'Ph' in input_data_name or 'Powheg' in input_data_name:
                    if (trf_cache == 'AtlasProduction' and LooseVersion(trf_release) >= LooseVersion('19.2.4.11')) or \
                            (trf_cache == 'MCProd' and LooseVersion(trf_release) >= LooseVersion('19.2.4.9.3')):
                        if 'inputGenConfFile' in input_params.keys():
                            use_evnt_txt = True
                        elif not 'inputGenConfFile' in input_params.keys() and \
                                not 'inputGeneratorFile' in input_params.keys():
                            use_evnt_txt = True
                    if use_evnt_txt:
                        if not 'TXT' in output_types:
                            pass
                    else:
                        if 'TXT' in output_types:
                            output_types.remove('TXT')
                if 'aMcAtNlo' in input_data_name:
                    if int(ctag_name[1:]) >= 6000 and \
                            project in ('mc15_13TeV', 'mc16_13TeV', 'mc15_valid', 'mc16_valid', 'mc15_5TeV'):
                        if 'inputGenConfFile' in input_params.keys():
                            use_evnt_txt = True
                        elif not 'inputGenConfFile' in input_params.keys() and \
                                not 'inputGeneratorFile' in input_params.keys():
                            use_evnt_txt = True
                    if use_evnt_txt:
                        if not 'TXT' in output_types:
                            pass
                    else:
                        if 'TXT' in output_types:
                            output_types.remove('TXT')

            skip_scout_jobs = None

            try:
                try:
                    oe = OpenEnded.objects.get(request__id=step.request.id)
                    if oe.status.lower() == 'open':
                        request_task_list = ProductionTask.objects.filter(request=step.request)
                        for prod_task in request_task_list:
                            task_output_types = prod_task.step.step_template.output_formats.split('.')
                            if set(task_output_types) == set(output_types):
                                jedi_task = TTask.objects.get(id=prod_task.id)
                                if jedi_task.status.lower() in (self.protocol.TASK_STATUS[TaskStatus.RUNNING],
                                                                self.protocol.TASK_STATUS[TaskStatus.FINISHED],
                                                                self.protocol.TASK_STATUS[TaskStatus.DONE]):
                                    if jedi_task.total_done_jobs > 0:
                                        break
                except ObjectDoesNotExist:
                    pass
            except Exception as ex:
                logger.exception("Checking OE failed: %s" % str(ex))

            if 'nEventsPerJob' in input_params.keys():
                task_config.update({'nEventsPerJob': int(input_params['nEventsPerJob'])})
                self._set_task_config(step, task_config)

            random_seed_offset = 0
            first_event_offset = 0
            skip_check_input_ne = False
            evgen_input_formats = list()

            if prod_step.lower() == 'evgen'.lower():
                evgen_number_input_files = 0
                for key in input_params.keys():
                    if re.match(r'^(--)?input.*File$', key, re.IGNORECASE):
                        for input_name in input_params[key]:
                            evgen_number_input_files += self.rucio_client.get_number_files(input_name)
                            try:
                                input_name_dict = self.parse_data_name(input_name)
                                if input_name_dict['prod_step'] == 'evgen':
                                    evgen_input_formats.append(input_name_dict['data_type'])
                            except Exception:
                                pass
                if number_of_events > 0 and task_config.get('nEventsPerJob', None) and not evgen_params:
                    events_per_job = int(task_config['nEventsPerJob'])
                    if not (number_of_events % events_per_job == 0):
                        raise Exception('The task is rejected because of inconsistency. ' +
                                        'nEvents={0}, nEventsPerJob={1}, step={2}'.format(
                                            number_of_events, events_per_job, prod_step))
                if evgen_number_input_files == 1:
                    events_per_job = int(task_config['nEventsPerJob'])
                    task_config.update({'split_slice': True})
                    self._set_task_config(step, task_config)
                    random_seed_offset = self._get_number_events_processed(step) / events_per_job
                    first_event_offset = random_seed_offset * events_per_job
                    skip_check_input = True
                    if number_of_events > 0:
                        task_config['nEventsPerInputFile'] = number_of_events
                        skip_check_input_ne = True
                elif evgen_number_input_files > 1:
                    if len(evgen_input_formats) == 1 and 'TXT' in evgen_input_formats:
                        if 'nEventsPerInputFile' in task_config.keys() and 'nEventsPerJob' in task_config.keys():
                            task_config['nEventsPerInputFile'] = int(task_config['nEventsPerJob'])
                    if 'nEventsPerInputFile' in task_config.keys() and task_config['nEventsPerInputFile'] > 0 \
                            and number_of_events > 0:
                        evgen_number_input_files_requested = number_of_events / task_config['nEventsPerInputFile']
                        if evgen_number_input_files_requested < evgen_number_input_files and not use_evnt_filter and \
                                not use_lhe_filter:
                            task_config['nFiles'] = evgen_number_input_files_requested
                elif evgen_number_input_files == 0:
                    skip_check_input_ne = True
                    events_per_job = int(task_config['nEventsPerJob'])
                    task_config.update({'split_slice': True})
                    self._set_task_config(step, task_config)
                    random_seed_offset = self._get_number_events_processed(step) / events_per_job
                    first_event_offset = random_seed_offset * events_per_job
                    # # FIXME: replace by _get_number_events_processed
                    # offset = 0
                    # ps1_task_list = TTaskRequest.objects.filter(~Q(status__in=['failed', 'broken', 'aborted', 'obsolete']),
                    #                                             project=step.request.project,
                    #                                             inputdataset=input_data_name,
                    #                                             ctag=step.step_template.ctag,
                    #                                             formats=step.step_template.output_formats)
                    # for ps1_task in ps1_task_list:
                    #     offset += int(ps1_task.total_req_jobs or 0)
                    #
                    # ps2_task_list = ProductionTask.objects.filter(~Q(status__in=['failed', 'broken', 'aborted', 'obsolete']),
                    #                                               project=step.request.project,
                    #                                               inputdataset=input_data_name,
                    #                                               step__step_template__ctag=step.step_template.ctag,
                    #                                               step__step_template__output_formats=step.step_template.output_formats)
                    # for ps2_task in ps2_task_list:
                    #     total_req_jobs = int(ps2_task.total_req_jobs or 0)
                    #     if not total_req_jobs:
                    #         events_per_job = int(task_config['nEventsPerJob'])
                    #         total_req_jobs = number_of_events / events_per_job
                    #     offset += total_req_jobs
                    #
                    # random_seed_offset = offset

                if 'nEventsPerJob' in task_config.keys() and number_of_events > 0:
                    evgen_number_jobs = number_of_events / int(task_config['nEventsPerJob'])
                    if evgen_number_jobs <= 10:
                        skip_scout_jobs = True

            if 'nEventsPerInputFile' in task_config.keys() and 'nEventsPerJob' in task_config.keys() and \
                    (not skip_check_input_ne) and not 'nEventsPerInputFile'.lower() in project_mode.keys():
                self._check_task_events_consistency(task_config)

            if train_production:
                reduction_conf = list()
                for output_type in output_types[:]:
                    if output_type.lower().startswith('DAOD_'.lower()):
                        reduction_conf.append(output_type.split('_')[-1])
                        # reversed_output_type = '_'.join(output_type.split('_')[::-1])
                        # output_types.remove(output_type)
                        # output_types.append(reversed_output_type)
                        # output_param_name = "--output{0}File".format(reversed_output_type)
                        output_param_name = "--output{0}File".format(output_type)
                        if not output_param_name in trf_params:
                            trf_params.append(output_param_name)
                for key in ctag.keys():
                    if re.match('^(--)?reductionConf', key, re.IGNORECASE):
                        ctag[key] = ' '.join(reduction_conf)
                        break

            # proto_fix
            if trf_name.lower() == 'HLTHistMerge_tf.py'.lower():
                # if 'HIST' in output_types:
                # output_types.remove('HIST')
                # output_types.append('HIST_MRG')
                if not '--inputHISTFile' in trf_params:
                    trf_params.append('--inputHISTFile')
                if not '--outputHIST_MRGFile' in trf_params:
                    trf_params.remove('--outputHISTFile')
                    trf_params.append('--outputHIST_MRGFile')
            elif trf_name.lower() == 'SkimNTUP_trf.py'.lower():
                for input_key in input_params.keys():
                    trf_params.append(input_key)
                for output_type in output_types:
                    trf_params.append("output%sFile" % output_type)
            elif trf_name.lower() == 'csc_MergeHIST_trf.py'.lower():
                if not '--inputHISTFile' in trf_params:
                    trf_params.append('--inputHISTFile')
                if not '--outputHISTFile' in trf_params:
                    trf_params.append('--outputHISTFile')

            if not 'log' in output_types:
                output_types.append('log')
            output_params = self._get_output_params(input_data_name,
                                                    output_types,
                                                    project,
                                                    prod_step,
                                                    ctag_name,
                                                    task_proto_id)

            # proto_fix
            if trf_name.lower() == 'TrainReco_tf.py'.lower():
                trf_params.extend(["--%s" % key for key in input_params.keys()])
                trf_params.extend(
                    ["--%s" % key for key in output_params.keys() if key.lower() != 'outputlogFile'.lower()])

            # proto_fix
            if trf_name.lower() == 'DigiMReco_trf.py'.lower():
                if not 'preExec' in trf_params:
                    trf_params.append('preExec')
                if not 'postExec' in trf_params:
                    trf_params.append('postExec')
                if not 'preInclude' in trf_params:
                    trf_params.append('preInclude')

            if change_output_type_dict:
                for key in change_output_type_dict.keys():
                    output_param_name = "output{0}File".format(key)
                    if output_param_name in output_params.keys():
                        output_params["output{0}File".format(change_output_type_dict[key])] = \
                            output_params.pop(output_param_name)

            no_input = True

            if train_production:
                trf_options.update({'separator': ' '})

            if parent_task_id > 0 and not use_real_nevents:
                try:
                    number_of_events_per_input_file = self.task_reg.get_task_parameter(parent_task_id, 'nEventsPerJob')
                    if not 'nEventsPerInputFile' in task_config.keys():
                        task_config.update({'nEventsPerInputFile': number_of_events_per_input_file})
                except:
                    pass

            # FIXME
            # primary_input_offset = 0
            if 'primaryInputOffset'.lower() in project_mode.keys():
                primary_input_offset = int(project_mode['primaryInputOffset'.lower()])
            if 'randomSeedOffset'.lower() in project_mode.keys():
                random_seed_offset = int(project_mode['randomSeedOffset'.lower()])

            # if not len(trf_params):
            #     raise Exception("List of transformation parameters is empty")

            random_seed_proto_key = TaskParamName.RANDOM_SEED
            if step.request.request_type.lower() == 'MC'.lower():
                random_seed_proto_key = TaskParamName.RANDOM_SEED_MC

            is_pile_task = False
            is_not_transient_output = False

            if step.step_template.step.lower() in ['Rec Merge'.lower(), 'Atlf Merge'.lower()]:
                is_not_transient_output = True

            job_parameters = list()

            output_trf_params = list()
            for output_type in output_types:
                if trf_name.lower() == 'Trig_reco_tf.py'.lower() and output_type == 'RAW':
                    output_type = 'BS'
                if trf_name.lower().startswith('TrigFTK'.lower()) and output_type == 'RAW':
                    output_type = 'BS'
                if output_type in change_output_type_dict.keys():
                    output_type = change_output_type_dict[output_type]
                param_names = \
                    [e for e in trf_params if re.match(r"^(--)?output%s.*File$" % output_type, e, re.IGNORECASE)]
                if not param_names:
                    continue
                for param_name in param_names:
                    output_trf_params.append(param_name)

            for name in trf_params:
                if re.match('^(--)?extraParameter$', name, re.IGNORECASE):
                    param_value = self._get_parameter_value(name, ctag)
                    if param_value and str(param_value).lower() != 'none':
                        if '=' in param_value:
                            param_dict = {'name': param_value.split('=')[0], 'value': param_value.split('=')[1]}
                            param_dict.update(trf_options)
                            job_parameters.append(
                                self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                            )
                        else:
                            for extra_param in param_value.split(' '):
                                if not extra_param in trf_params:
                                    trf_params.append(extra_param)
                    break

            # FIXME
            # BS (byte stream) - for all *RAW* (DRAW, RAW, DRAW_ZEE, etc.) [1]
            name = self._get_input_output_param_name(input_params, 'RAW')
            if not name:
                name = self._get_input_output_param_name(input_params, 'DRAW')
            if name:
                input_bs_type = 'inputBSFile'
                if not input_bs_type in input_params.keys():
                    input_params[input_bs_type] = list()
                # input_params[input_bs_type].extend(input_params[name])
                for input_param_value in input_params[name]:
                    if not input_param_value in input_params[input_bs_type]:
                        input_params[input_bs_type].append(input_param_value)
                del input_params[name]

            if empty_trf_params:
                for param in empty_trf_params:
                    if param:
                        for trf_param in trf_params[:]:
                            if re.match(r"^(--)?%s$" % param, trf_param, re.IGNORECASE):
                                param_dict = {'name': trf_param, 'value': '""'}
                                param_dict.update(trf_options)
                                job_parameters.append(self.protocol.render_param(TaskParamName.CONSTANT, param_dict))
                                logger.info(
                                    'TRF parameter {0} is used with an empty value'.format(trf_param)
                                )
                                trf_params.remove(trf_param)

            if ignore_trf_params:
                for param in ignore_trf_params:
                    if param:
                        for trf_param in trf_params[:]:
                            if re.match(r"^(--)?%s$" % param, trf_param, re.IGNORECASE):
                                logger.info(
                                    'TRF parameter {0} is removed from the list. It is ignored'.format(trf_param)
                                )
                                trf_params.remove(trf_param)

            for name in trf_params:
                if re.match(r'^(--)?runNumber$', name, re.IGNORECASE):
                    run_number = input_data_dict['number']
                    try:
                        param_dict = {'name': name, 'value': int(run_number)}
                    except Exception as ex:
                        logger.exception("Exception occurred during obtaining runNumber: %s" % str(ex))
                        continue
                    param_dict.update(trf_options)
                    job_parameters.append(self.protocol.render_param(TaskParamName.CONSTANT, param_dict))
                elif re.match(r'^(--)?amiTag$', name, re.IGNORECASE):
                    param_dict = {'name': name, 'value': ctag_name}
                    param_dict.update(trf_options)
                    job_parameters.append(self.protocol.render_param(TaskParamName.CONSTANT, param_dict))
                elif re.match(r'^(--)?geometryversion$', name, re.IGNORECASE):
                    param_value = self._get_parameter_value('geometry', ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        param_value = self._get_parameter_value(name, ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    param_dict = {'name': name, 'value': param_value}
                    param_dict.update(trf_options)
                    job_parameters.append(self.protocol.render_param(TaskParamName.CONSTANT, param_dict))
                elif re.match(r'^(--)?DBRelease$', name, re.IGNORECASE):
                    param_value = self._get_parameter_value(name, ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        project_mode['ipConnectivity'.lower()] = 'http'
                        continue
                    if not re.match(r'^\d+(\.\d+)*$', param_value):
                        if param_value.lower() == 'latest'.lower():
                            param_dict = {'name': name, 'dataset': self._get_latest_db_release()}
                            param_dict.update(trf_options)
                            job_parameters.append(
                                self.protocol.render_param(TaskParamName.DB_RELEASE, param_dict)
                            )
                        else:
                            param_dict = {'name': name, 'value': param_value}
                            param_dict.update(trf_options)
                            job_parameters.append(
                                self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                            )
                    else:
                        if trf_name.lower().find('_tf.') >= 0:
                            # --DBRelease=x.x.x
                            param_dict = {'name': name, 'value': param_value}
                            param_dict.update(trf_options)
                            job_parameters.append(
                                self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                            )
                        else:
                            db_rel_version = ''.join(["%2.2i" % int(i) for i in param_value.split('.')])
                            db_rel_name = \
                                "%s%s" % (TaskDefConstants.DEFAULT_DB_RELEASE_DATASET_NAME_BASE, db_rel_version)
                            param_dict = {'name': name,
                                          'dataset': db_rel_name}
                            param_dict.update(trf_options)
                            job_parameters.append(
                                self.protocol.render_param(TaskParamName.DB_RELEASE, param_dict)
                            )
                elif re.match(r'^(--)?jobConfig', name, re.IGNORECASE):
                    param_value = self._get_parameter_value(name, input_params)
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    param_dict = {'name': name, 'value': param_value}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                    )
                elif re.match(r'^(--)?skipEvents$', name, re.IGNORECASE):
                    if skip_events_forced >= 0:
                        param_dict = {'name': name, 'value': skip_events_forced}
                        param_dict.update(trf_options)
                        job_parameters.append(
                            self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                        )
                        continue
                    else:
                        param_value = self._get_parameter_value(name, ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        if ('nEventsPerJob' in task_config.keys() or 'nEventsPerRange' in task_config.keys() or
                            'tgtNumEventsPerJob'.lower() in project_mode.keys()) and \
                                ('nEventsPerInputFile' in task_config.keys() or use_real_nevents):
                            param_dict = {'name': name}
                            param_dict.update(trf_options)
                            job_parameters.append(
                                self.protocol.render_param(TaskParamName.SKIP_EVENTS, param_dict)
                            )
                        else:
                            logger.warning('skipEvents parameter is omitted (step={0})'.format(step.id))
                        continue
                    param_dict = {'name': name, 'value': param_value}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                    )
                elif re.match(r'^(--)?randomSeed$', name, re.IGNORECASE):
                    param_dict = {'name': name, 'offset': random_seed_offset}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(random_seed_proto_key, param_dict)
                    )
                elif re.match(r'^(--)?digiSeedOffset1$', name, re.IGNORECASE) or \
                        re.match(r'^(--)?digiSeedOffset2$', name, re.IGNORECASE):
                    input_real_data = False
                    # or check .RAW in the end: data12_8TeV.00208811.physics_JetTauEtmiss.merge.RAW?
                    for key in input_params.keys():
                        if re.match(r'^(--)?inputBSFile$', key, re.IGNORECASE) or \
                                re.match(r'^(--)?inputRAWFile$', key, re.IGNORECASE):
                            input_real_data = True
                    if input_real_data:
                        continue
                    param_dict = {'name': name, 'offset': random_seed_offset}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(random_seed_proto_key, param_dict)
                    )
                elif re.match(r'^(--)?maxEvents$', name, re.IGNORECASE):
                    if max_events_forced:
                        param_value = max_events_forced
                    else:
                        param_value = self._get_parameter_value(name, ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        if ('nEventsPerJob' in task_config.keys() or 'nEventsPerRange' in task_config.keys() or
                            'tgtNumEventsPerJob'.lower() in project_mode.keys()) and \
                                ('nEventsPerInputFile' in task_config.keys() or use_real_nevents):
                            param_dict = {'name': name}
                            param_dict.update(trf_options)
                            job_parameters.append(
                                self.protocol.render_param(TaskParamName.MAX_EVENTS, param_dict)
                            )
                        else:
                            logger.warning('maxEvents parameter is omitted (step={0})'.format(step.id))
                        continue
                    param_dict = {'name': name, 'value': param_value}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                    )
                elif re.match('^(--)?firstEvent$', name, re.IGNORECASE):
                    param_value = self._get_parameter_value(name, ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        if ('nEventsPerJob' in task_config.keys() or 'nEventsPerRange' in task_config.keys() or
                            'tgtNumEventsPerJob'.lower() in project_mode.keys()) and \
                                ('nEventsPerInputFile' in task_config.keys() or use_real_nevents):
                            param_dict = {'name': name, 'offset': 0}
                            if prod_step.lower() == 'evgen'.lower():
                                param_dict.update({'offset': first_event_offset})
                            param_dict.update(trf_options)
                            job_parameters.append(
                                self.protocol.render_param(TaskParamName.FIRST_EVENT, param_dict)
                            )
                        else:
                            logger.warning("firstEvents parameter is omitted" % step.id)
                        continue
                    param_dict = {'name': name, 'value': param_value}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                    )
                elif re.match('^(--)?extraParameter$', name, re.IGNORECASE):
                    continue
                # FIXME: OverlayTest
                elif re.match('^(--)?input(Filter|VertexPos)File$', name, re.IGNORECASE):
                    param_value = self._get_parameter_value(name, ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    if overlay_production:
                        proto_key = TaskParamName.OVERLAY_FILTER_FILE
                        param_dict = {'name': name, 'task_id': task_proto_id}
                    else:
                        proto_key = TaskParamName.CONSTANT
                        param_dict = {'name': name, 'value': param_value}
                    param_dict.update(trf_options)
                    job_parameters.append(self.protocol.render_param(proto_key, param_dict))
                elif re.match('^(--)?hitarFile$', name, re.IGNORECASE):
                    param_value = self._get_parameter_value(name, ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    param_dict = {'name': name, 'dataset': param_value}
                    param_dict.update(trf_options)
                    param = self.protocol.render_param(TaskParamName.HITAR_FILE, param_dict)
                    nf = self.rucio_client.get_number_files(param_value)
                    if nf > 1:
                        param['attributes'] = ''
                        param['ratio'] = 1
                    if secondary_input_offset:
                        param['offset'] = secondary_input_offset
                    job_parameters.append(param)
                elif re.match('^(--)?inputZeroBiasBSFile$', name, re.IGNORECASE) \
                        or re.match('^(--)inputRDO_BKGFile?$', name, re.IGNORECASE):
                    param_value = self._get_parameter_value(name, ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    if param_value[-1] != '/' and (not '_tid' in param_value):
                        if self.rucio_client.is_dsn_container(param_value):
                            param_value = '%s/' % param_value
                    param_dict = {'name': name, 'dataset': param_value}
                    param_dict.update(trf_options)
                    if 'eventRatio'.lower() in project_mode.keys():
                        event_ratio = float(project_mode['eventRatio'.lower()]) \
                            if '.' in str(project_mode['eventRatio'.lower()]) else int(
                            project_mode['eventRatio'.lower()])
                        param_dict.update({'event_ratio': event_ratio})
                    second_input_param = \
                        self.protocol.render_param(TaskParamName.SECONDARY_INPUT_ZERO_BIAS_BS, param_dict)
                    n_pileup = TaskDefConstants.DEFAULT_MINIBIAS_NPILEUP
                    if 'npileup' in project_mode.keys():
                        n_pileup = float(project_mode['npileup']) \
                            if '.' in str(project_mode['npileup']) else int(project_mode['npileup'])
                    second_input_param['ratio'] = n_pileup
                    if secondary_input_offset:
                        second_input_param['offset'] = secondary_input_offset
                    job_parameters.append(second_input_param)
                    is_pile_task = True
                elif re.match(r'^.*(PtMinbias|Cavern).*File$', name, re.IGNORECASE):
                    param_name = name
                    if not self.ami_client.is_new_ami_tag(ctag):
                        if re.match(r'^(--)?input(Low|High)PtMinbias.*File$', name, re.IGNORECASE):
                            name = name.replace('--', '').replace('input', '')
                        if re.match(r'^(--)?inputCavern.*File$', name, re.IGNORECASE):
                            name = name.replace('--', '').replace('input', '')
                    param_value = self._get_parameter_value(name, ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    if param_value[-1] != '/' and (not '_tid' in param_value):
                        param_value = '%s/' % param_value
                    postfix = ''
                    # if name.lower().startswith('Low'.lower()):
                    if 'Low'.lower() in name.lower():
                        postfix = '_LOW'
                    # elif name.lower().startswith('High'.lower()):
                    elif 'High'.lower() in name.lower():
                        postfix = '_HIGH'
                    param_dict = {'name': param_name, 'dataset': param_value, 'postfix': postfix}
                    param_dict.update(trf_options)
                    if 'eventRatio'.lower() in project_mode.keys():
                        event_ratio = float(project_mode['eventRatio'.lower()]) \
                            if '.' in str(project_mode['eventRatio'.lower()]) else int(
                            project_mode['eventRatio'.lower()])
                        param_dict.update({'event_ratio': event_ratio})
                    if postfix == '_LOW':
                        if 'eventRatioLow'.lower() in project_mode.keys():
                            event_ratio = float(project_mode['eventRatioLow'.lower()]) \
                                if '.' in str(project_mode['eventRatioLow'.lower()]) else int(
                                project_mode['eventRatioLow'.lower()])
                            param_dict.update({'event_ratio': event_ratio})
                    elif postfix == '_HIGH':
                        if 'eventRatioHigh'.lower() in project_mode.keys():
                            event_ratio = float(project_mode['eventRatioHigh'.lower()]) \
                                if '.' in str(project_mode['eventRatioHigh'.lower()]) else int(
                                project_mode['eventRatioHigh'.lower()])
                            param_dict.update({'event_ratio': event_ratio})
                    if 'Cavern'.lower() in name.lower():
                        second_input_param = self.protocol.render_param(TaskParamName.SECONDARY_INPUT_CAVERN,
                                                                        param_dict)
                    else:
                        second_input_param = self.protocol.render_param(TaskParamName.SECONDARY_INPUT_MINBIAS,
                                                                        param_dict)
                    n_pileup = TaskDefConstants.DEFAULT_MINIBIAS_NPILEUP
                    if 'npileup' in project_mode.keys():
                        n_pileup = int(project_mode['npileup'])
                    if postfix == '_LOW':
                        if 'npileuplow' in project_mode.keys():
                            n_pileup = float(project_mode['npileuplow']) \
                                if '.' in str(project_mode['npileuplow']) else int(project_mode['npileuplow'])
                    elif postfix == '_HIGH':
                        if 'npileuphigh' in project_mode.keys():
                            n_pileup = float(project_mode['npileuphigh']) \
                                if '.' in str(project_mode['npileuphigh']) else int(project_mode['npileuphigh'])
                    second_input_param['ratio'] = n_pileup
                    if secondary_input_offset:
                        second_input_param['offset'] = secondary_input_offset
                    job_parameters.append(second_input_param)
                    is_pile_task = True
                elif re.match(r'^(--)?input.*File$', name, re.IGNORECASE):
                    param_name = re.sub("(?<=input)evgen(?=file)", "EVNT".lower(), name.lower())
                    # BS (byte stream) - for all *RAW* (DRAW, RAW, DRAW_ZEE, etc.) [2]
                    if re.match(r'^(--)?inputBSFile$', name, re.IGNORECASE) and 'RAW'.lower() in ','.join(
                            [e.lower() for e in input_params.keys()]):
                        param_name = self._get_input_output_param_name(input_params, 'RAW')
                        if not param_name:
                            param_name = self._get_input_output_param_name(input_params, 'DRAW')
                            if not param_name:
                                continue
                    if re.match(r'^(--)?inputESDFile$', name, re.IGNORECASE) and 'ESD'.lower() in ','.join(
                            [e.lower() for e in input_params.keys()]):
                        param_name = self._get_input_output_param_name(input_params, 'ESD')
                        if not param_name:
                            param_name = self._get_input_output_param_name(input_params, 'DESD')
                            if not param_name:
                                continue
                    if re.match(r'^(--)?inputLogsFile$', name, re.IGNORECASE) and 'log'.lower() in ','.join(
                            [e.lower() for e in input_params.keys()]):
                        param_name = self._get_input_output_param_name(input_params, 'log')
                        if not param_name:
                            continue
                    if re.match(r'^(--)?inputHISTFile', name, re.IGNORECASE) and 'HIST'.lower() in ','.join(
                            [e.lower() for e in input_params.keys()]):
                        param_name = self._get_input_output_param_name(input_params, 'HIST')
                        if not param_name:
                            continue
                    if re.match(r'^(--)?input(AOD|POOL)File$', name, re.IGNORECASE) and 'AOD'.lower() in ','.join(
                            [e.lower() for e in input_params.keys()]):
                        param_name = self._get_input_output_param_name(input_params, 'AOD')
                        if not param_name:
                            param_name = self._get_input_output_param_name(input_params, 'DAOD')
                            if not param_name:
                                continue
                    if re.match(r'^(--)?inputDataFile$', name, re.IGNORECASE):
                        param_name = self._get_input_output_param_name(input_params, '')
                    param_value = list(self._get_parameter_value(param_name, input_params))
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    if not len(param_value):
                        continue
                    if len(param_value) > 1:
                        param_value = "{{%s_dataset}}" % self._get_parameter_name(param_name, input_params)
                    else:
                        param_value = param_value[0]

                    postfix = ''

                    try:
                        result = re.match(r'^(--)?input(?P<intype>.*)File$', name, re.IGNORECASE)
                        if result:
                            postfix = "_%s" % result.groupdict()['intype']
                            postfix = postfix.upper()
                    except:
                        pass

                    param_dict = {'name': name, 'dataset': param_value, 'postfix': postfix}
                    param_dict.update(trf_options)
                    if use_direct_io:
                        input_param = self.protocol.render_param(TaskParamName.INPUT_DIRECT_IO, param_dict)
                    else:
                        input_param = self.protocol.render_param(TaskParamName.INPUT, param_dict)
                    job_parameters.append(input_param)
                    no_input = False
                elif re.match(r'^(--)?outputDAODFile$', name, re.IGNORECASE) and train_production:
                    param_dict = {'name': name,
                                  'task_id': task_proto_id}
                    param_dict.update(trf_options)
                    merge_tag_name = self._get_merge_tag_name(step)
                    if merge_tag_name:
                        param_proto_key = TaskParamName.TRAIN_DAOD_FILE_JEDI_MERGE
                    else:
                        param_proto_key = TaskParamName.TRAIN_DAOD_FILE
                    job_parameters.append(self.protocol.render_param(param_proto_key, param_dict))
                elif re.match(r'^(--)?output.*File$', name, re.IGNORECASE):
                    if use_no_output:
                        continue
                    if output_trf_params and not name in output_trf_params:
                        continue
                    param_name = name
                    if re.match(r'^(--)?output.*_MRGFile$', name, re.IGNORECASE):
                        result = re.match(r'^(--)?output(?P<type>\w+)_MRGFile$', name, re.IGNORECASE)
                        if result:
                            internal_type = result.groupdict()['type']
                            if internal_type.lower() == 'BS'.lower():
                                internal_type = 'RAW'
                            param_name = self._get_input_output_param_name(output_params, internal_type)
                            if not param_name:
                                param_name = self._get_input_output_param_name(output_params, "D%s" % internal_type)
                                if not param_name:
                                    continue
                    elif re.match(r'^(--)?outputBS.*File$', name, re.IGNORECASE) and 'RAW'.lower() in ','.join(
                            [e.lower() for e in output_params.keys()]):
                        param_name = self._get_input_output_param_name(output_params, 'RAW')
                        if not param_name:
                            continue
                            # param_name = self._get_input_output_param_name(output_params, 'DRAW')
                            # if not param_name:
                            #     continue
                    elif re.match(r'^(--)?outputAODFile$', name, re.IGNORECASE) and 'AOD'.lower() in ','.join(
                            [e.lower() for e in output_params.keys()]):
                        if train_production:
                            continue
                        param_name = self._get_input_output_param_name(output_params, 'AOD')
                        if not param_name:
                            continue
                    elif re.match(r'^(--)?outputESDFile$', name, re.IGNORECASE) and 'ESD'.lower() in ','.join(
                            [e.lower() for e in output_params.keys()]):
                        param_name = self._get_input_output_param_name(output_params, 'ESD')
                        if not param_name:
                            continue
                    elif re.match(r'^(--)?outputDAODFile$', name, re.IGNORECASE) and 'DAOD'.lower() in ','.join(
                            [e.lower() for e in output_params.keys()]):
                        param_name = self._get_input_output_param_name(output_params, 'DAOD')
                        if not param_name:
                            continue
                    elif re.match(r'^(--)?outputHITS.*File$', name, re.IGNORECASE) and 'HITS'.lower() in ','.join(
                            [e.lower() for e in output_params.keys()]):
                        param_name = self._get_input_output_param_name(output_params, 'HITS')
                        if not param_name:
                            continue
                    elif re.match(r'^(--)?outputArchFile$', name, re.IGNORECASE):
                        param_name = self._get_input_output_param_name(output_params, output_types[0])
                    param_value = list(self._get_parameter_value(param_name, output_params))
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    if not len(param_value):
                        continue
                    param_value = param_value[0]
                    output_dataset_dict = self.parse_data_name(param_value)
                    output_data_type = output_dataset_dict['data_type']
                    param_dict = {'name': name,
                                  'dataset': param_value,
                                  'data_type': output_data_type,
                                  'task_id': task_proto_id}
                    param_dict.update(trf_options)
                    proto_key = TaskParamName.OUTPUT
                    if train_production and output_data_type.split('_')[0] == 'DAOD':
                        proto_key = TaskParamName.TRAIN_OUTPUT
                    # FIXME: OverlayTest
                    elif re.match(r'^(--)?outputTXT_EVENTIDFile$', name, re.IGNORECASE):
                        proto_key = TaskParamName.TXT_EVENTID_OUTPUT
                    elif re.match(r'^(--)?outputTXT.*File$', name, re.IGNORECASE):
                        proto_key = TaskParamName.TXT_OUTPUT
                    elif re.match(r'^(--)?outputTAR_CONFIGFile$', name, re.IGNORECASE):
                        proto_key = TaskParamName.TAR_CONFIG_OUTPUT
                    elif re.match(r'^(--)?outputArchFile$', name, re.IGNORECASE):
                        proto_key = TaskParamName.ZIP_OUTPUT
                        arch_param_dict = {'idx': 0}
                        arch_proto_key = TaskParamName.ZIP_MAP
                        arch_param = self.protocol.render_param(arch_proto_key, arch_param_dict)
                        job_parameters.append(arch_param)
                    output_param = self.protocol.render_param(proto_key, param_dict)
                    if 'spacetoken' in project_mode.keys():
                        output_param['token'] = project_mode['spacetoken']
                    if 'token' in task_config.keys():
                        output_param['token'] = task_config['token']
                    # output_param['destination'] = 'UKI-NORTHGRID-LIV-HEP_SL6'
                    if train_production and output_data_type.split('_')[0] == 'DAOD':
                        output_param['hidden'] = True
                    if is_not_transient_output:
                        output_param['transient'] = not is_not_transient_output
                    if allow_no_output_patterns:
                        for pattern in allow_no_output_patterns:
                            if re.match(r'^(--)?output%sFile$' % pattern, name, re.IGNORECASE):
                                output_param['allowNoOutput'] = True
                                logger.info("Output parameter {0} has attribute allowNoOutput=True".format(name))
                                break
                    if hidden_output_patterns:
                        for pattern in hidden_output_patterns:
                            if re.match(r'^(--)?output%sFile$' % pattern, name, re.IGNORECASE):
                                output_param['hidden'] = True
                                logger.info("Output parameter {0} has attribute hidden=True".format(name))
                                break
                    if output_ratio > 0:
                        output_param['ratio'] = output_ratio
                    job_parameters.append(output_param)
                elif re.match('^(--)?jobNumber$', name, re.IGNORECASE):
                    if trf_name.lower() == 'AtlasG4_tf.py'.lower():
                        continue
                    elif trf_name.lower() == 'Sim_tf.py'.lower():
                        continue
                    param_dict = {'name': name, 'offset': random_seed_offset}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(random_seed_proto_key, param_dict)
                    )
                elif re.match('^(--)?filterFile$', name, re.IGNORECASE):
                    param_value = self._get_parameter_value(name, ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    dataset = param_value
                    run_number_str = input_data_dict['number']
                    filenames = self.rucio_client.list_files_in_dataset(dataset)
                    filter_filename = None
                    for filename in filenames:
                        if run_number_str in filename:
                            filter_filename = filename
                            break
                    if not filter_filename:
                        logger.info("Step = %d, filter file is not found" % step.id)
                        continue
                    param_dict = {'name': name, 'dataset': dataset, 'filename': filter_filename, 'ratio': 1,
                                  'files': [{'lfn': filter_filename}]}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(TaskParamName.FILTER_FILE, param_dict)
                    )
                else:
                    param_value = self._get_parameter_value(name, ctag, sub_steps=trf_sub_steps)
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    if str(param_value).lower() == 'none,none':
                        continue
                    param_dict = {'name': name, 'value': param_value}
                    param_dict.update(trf_options)

                    if not trf_sub_steps is None:
                        for trf_sub_step in trf_sub_steps:
                            if "%s:" % trf_sub_step in param_value:
                                param_dict.update({'separator': ' '})
                                break

                    if re.match('^(--)?validationFlags', name, re.IGNORECASE):
                        param_dict.update({'separator': ' '})
                    elif re.match('^(--)?skipFileValidation', name, re.IGNORECASE):
                        if param_value.lower() == 'True'.lower():
                            param_dict.update({'separator': ''})
                            param_dict.update({'value': ''})
                        else:
                            continue
                    elif re.match('^(--)?athenaMPMergeTargetSize', name, re.IGNORECASE):
                        param_dict.update({'separator': ' '})
                    elif re.match('^(--)?(steering|triggerConfig)', name, re.IGNORECASE):
                        if ' ' in param_value:
                            param_dict.update({'separator': ' '})

                    job_parameters.append(
                        self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                    )

            if not job_parameters:
                raise Exception("List of task parameters is empty")

            no_output = True
            input_types_defined = list()
            output_types_defined = list()
            output_types_defined.append('log')
            for job_param in job_parameters:
                if index_consistent_param_list:
                    name = job_param.get('value', '').split('=')[0].replace('--', '')
                    if name in index_consistent_param_list or '--{0}'.format(name) in index_consistent_param_list:
                        job_param['indexConsistent'] = True
                if re.match(r'^(--)?input.*File', job_param['value'], re.IGNORECASE):
                    result = re.match(r'^(--)?input(?P<intype>.*)File', job_param['value'], re.IGNORECASE)
                    if not result:
                        continue
                    in_type = result.groupdict()['intype']
                    if in_type.lower() == 'logs'.lower() or \
                            re.match(r'^.*(PtMinbias|Cavern|ZeroBiasBS|HITAR|Filter|RDO_BKG).*$', in_type,
                                     re.IGNORECASE):
                        continue
                    input_types_defined.append(in_type)
                    # moving primary input parameter
                    job_parameters.remove(job_param)
                    job_parameters.insert(0, job_param)
                if not 'param_type' in job_param.keys():
                    continue
                if job_param['param_type'].lower() == 'output'.lower():
                    no_output = False
                    if 'dataset' in job_param.keys():
                        output_dataset_dict = self.parse_data_name(job_param['dataset'])
                        output_types_defined.append(output_dataset_dict['data_type'])

            if no_output and not use_no_output:
                raise Exception("Output data are missing")

            output_types_not_defined = list(set(output_types).difference(set(output_types_defined)))
            if output_types_not_defined and not use_no_output:
                message = 'These requested outputs are not defined properly: {0}.' \
                    .format('.'.join(output_types_not_defined))
                param_names = \
                    [e.replace('--', '') for e in trf_params if re.match(r"^(--)?output.*File$", e, re.IGNORECASE)]
                message = '{0}\n[tag = {1} ({2},{3}_{4})] {2} supports only these output parameters: {5}' \
                    .format(message, ctag_name, trf_name, trf_cache, trf_release, ','.join(param_names))
                raise Exception(message)

            log_param_dict = {'dataset': output_params['outputlogFile'][0], 'task_id': task_proto_id}
            log_param_dict.update(trf_options)
            log_param = self.protocol.render_param(TaskParamName.LOG, log_param_dict)
            if is_not_transient_output:
                log_param['transient'] = not is_not_transient_output
            if leave_log:
                self.protocol.set_leave_log_param(log_param)
            if 'token' in task_config.keys():
                if step.request.request_type.lower() in ['GROUP'.lower()]:
                    log_param['token'] = task_config['token']

            if 'Data' in input_types_defined:
                for job_param in job_parameters:
                    if re.match(r'^(--)?inputDataFile', job_param['value'], re.IGNORECASE):
                        if len(input_types_defined) > 1:
                            job_parameters.remove(job_param)
                            break

            if trf_name.lower() == 'DigiMReco_trf.py'.lower():
                if not 'outputESDFile' in output_params.keys():
                    param_dict = {'name': 'outputESDFile', 'value': 'ESD.TMP._0000000_tmp.pool.root'}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                    )
            elif trf_name.lower() == 'Trig_reco_tf.py'.lower():
                # FIXME: support for HLT reprocessing
                # if not 'RAW' in [e.lower() for e in output_params.keys()]:
                # if not 'RAW' in output_types:
                #     param_dict = {'name': '--outputBSFile', 'value': 'temp.BS'}
                #     param_dict.update(trf_options)
                #     job_parameters.append(
                #         self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                #     )
                for job_param in job_parameters[:]:
                    if re.match('^(--)?jobNumber$', job_param['value'], re.IGNORECASE):
                        job_parameters.remove(job_param)
                        break
            elif trf_name.lower() == 'csc_MergeHIST_trf.py'.lower():
                for job_param in job_parameters[:]:
                    job_param['value'] = job_param['value'].split('=')[-1]
            elif trf_name.lower() == 'POOLtoEI_tf.py'.lower():
                if use_no_output:
                    param_dict = {'name': '--outputEIFile', 'value': 'temp.ei.spb'}
                    if ei_output_filename:
                        param_dict = {'name': '--outputEIFile', 'value': ei_output_filename}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                    )

            if env_params_dict:
                for key in env_params_dict.keys():
                    param_dict = {'name': '--env {0}'.format(key), 'value': env_params_dict[key]}
                    options = {'separator': '='}
                    param_dict.update(options)
                    job_parameters.append(
                        self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                    )

            if 'reprocessing' in project_mode.keys() or (step.request.phys_group.lower() == 'REPR'.lower()):
                task_type = 'reprocessing'
            else:
                task_type = prod_step
                if is_pile_task:
                    task_type = 'pile'
            if prod_step.lower() == 'archive'.lower():
                task_type = prod_step

            campaign = ':'.join(filter(None, (step.request.campaign, step.request.subcampaign, bunchspacing,)))

            task_request_type = None
            if step.request.request_type.lower() == 'TIER0'.lower():
                task_request_type = 'T0spillover'

            task_trans_home_separator = '-'
            task_release = trf_release
            task_release_base = trf_release_base
            trans_uses_prefix = ''

            if use_nightly_release:
                task_trans_home_separator = '/'
                task_release = version_timestamp
                task_release_base = version_major

            task_proto_dict = {
                'trans_home_separator': task_trans_home_separator,
                'trans_uses_prefix': trans_uses_prefix,
                'job_params': job_parameters,
                'log': log_param,
                'architecture': project_mode['cmtconfig'],
                'type': task_type,
                'taskname': taskname,
                'priority': priority,
                'cache': trf_cache,
                'release': task_release,
                'transform': trf_name,
                'release_base': task_release_base,
                'username': username,
                'usergroup': usergroup,
                'no_wait_parent': True,
                'max_attempt': TaskDefConstants.DEFAULT_MAX_ATTEMPT,
                'skip_scout_jobs': skip_scout_jobs,
                'campaign': campaign,  # step.request.subcampaign,
                'req_id': int(step.request.id),
                'prod_source': TaskDefConstants.DEFAULT_PROD_SOURCE,
                'use_real_nevents': use_real_nevents,
                'cpu_time_unit': 'HS06sPerEvent',
                'write_input_to_file': use_direct_io,
                'cloud': TaskDefConstants.DEFAULT_CLOUD,
                'reuse_sec_on_demand': True if is_pile_task else None,
                'request_type': task_request_type,
                'scout_success_rate': TaskDefConstants.DEFAULT_SCOUT_SUCCESS_RATE
            }

            core_count = 1
            if 'coreCount'.lower() in project_mode.keys():
                core_count = int(project_mode['coreCount'.lower()])
                task_proto_dict.update({'number_of_cpu_cores': core_count})

            # https://twiki.cern.ch/twiki/bin/view/AtlasComputing/ProdSys#Default_cpuTime_cpu_TimeUnit_tab
            # https://twiki.cern.ch/twiki/bin/view/AtlasComputing/ProdSys#Default_base_RamCount_ramCount_r
            if step.request.request_type.lower() == 'MC'.lower():
                if prod_step.lower() == 'simul'.lower():
                    # simulation_type = self.protocol.get_simulation_type(step)
                    # if simulation_type == 'fast':
                    #     task_proto_dict.update({'cpu_time': 300})
                    # else:
                    #     task_proto_dict.update({'cpu_time': 3000})
                    task_proto_dict.update({'cpu_time': 3000})
                    task_proto_dict.update({'cpu_time_unit': 'HS06sPerEvent'})
                    if core_count > 1:
                        memory = 500
                        base_memory = 1000
                elif prod_step.lower() == 'recon'.lower() or is_pile_task:
                    if core_count > 1:
                        memory = 2000
                        base_memory = 2000
            elif step.request.request_type.lower() == 'HLT'.lower():
                if prod_step.lower() == 'recon'.lower():
                    task_proto_dict.update({'cpu_time': 300})
                    task_proto_dict.update({'cpu_time_unit': 'HS06sPerEvent'})
                    memory = 4000
                elif prod_step.lower() == 'merge'.lower():
                    if 'HIST'.lower() in '.'.join([e.lower() for e in output_types_defined]):
                        task_proto_dict.update({'cpu_time': 0})
                    else:
                        task_proto_dict.update({'cpu_time': 1})
                    task_proto_dict.update({'cpu_time_unit': 'HS06sPerEvent'})
            elif step.request.request_type.lower() == 'GROUP'.lower():
                task_proto_dict.update({'cpu_time': 0})
                task_proto_dict.update({'cpu_time_unit': 'HS06sPerEvent'})
                if project.lower().startswith('data'):
                    task_proto_dict.update({'goal': str(100.0)})
                    task_proto_dict.update({'use_exhausted': True})

            if trf_name in ['AODMerge_tf.py', 'DAODMerge_tf.py', 'Archive_tf.py', 'ESDMerge_tf.py']:
                task_proto_dict.update({'out_disk_count': 1000})
                task_proto_dict.update({'out_disk_unit': 'kB'})

            task_proto_dict.update({'ram_count': int(memory)})
            task_proto_dict.update({'base_ram_count': int(base_memory)})
            task_proto_dict.update({'ram_unit': 'MBPerCore'})

            if step.request.request_type.lower() == 'GROUP'.lower():
                task_proto_dict.update({'respect_split_rule': True})

            if 'ramCount'.lower() in project_mode.keys():
                task_proto_dict.update({'ram_count': int(project_mode['ramCount'.lower()])})
            if 'baseRamCount'.lower() in project_mode.keys():
                task_proto_dict.update({'base_ram_count': int(project_mode['baseRamCount'.lower()])})

            # if 'cloud'.lower() in project_mode.keys():
            #     task_proto_dict.update({'cloud': str(project_mode['cloud'.lower()])})

            if 'site'.lower() in project_mode.keys():
                site_value = str(project_mode['site'.lower()])
                specified_sites = list()
                if ',' in site_value:
                    specified_sites.extend(site_value.split(','))
                else:
                    specified_sites.append(site_value)
                available_sites = self.agis_client.get_sites()
                for site_name in specified_sites:
                    if not site_name in available_sites:
                        raise UnknownSiteException(site_name)
                task_proto_dict.update({'site': site_value})

            # if (not task_proto_dict.get('site', None)) and (not task_proto_dict.get('cloud', None)):
            #     task_proto_dict.update({'cloud': TaskDefConstants.DEFAULT_CLOUD})
            #     logger.info("cloud={0}".format(task_proto_dict['cloud']))

            # if 'maxAttempt'.lower() in project_mode.keys():
            #     task_proto_dict.update({'max_attempt': int(project_mode['maxAttempt'.lower()])})

            if 'disableReassign'.lower() in project_mode.keys():
                option_value = str(project_mode['disableReassign'.lower()])
                if option_value.lower() == 'yes'.lower():
                    task_proto_dict.update({'disable_reassign': True})
                elif option_value.lower() == 'no'.lower():
                    task_proto_dict.update({'disable_reassign': None})

            if 'skipScout'.lower() in project_mode.keys():
                option_value = str(project_mode['skipScout'.lower()])
                if option_value.lower() == 'yes'.lower():
                    task_proto_dict.update({'skip_scout_jobs': True})
                elif option_value.lower() == 'no'.lower():
                    task_proto_dict.update({'skip_scout_jobs': None})

            if 't1Weight'.lower() in project_mode.keys():
                task_proto_dict.update({'t1_weight': int(project_mode['t1Weight'.lower()])})

            if 'lumiblock'.lower() in project_mode.keys():
                option_value = str(project_mode['lumiblock'.lower()])
                if option_value.lower() == 'yes'.lower():
                    task_proto_dict.update({'respect_lb': True})
                elif option_value.lower() == 'no'.lower():
                    task_proto_dict.update({'respect_lb': None})

            if project.lower() == 'mc14_ruciotest'.lower():
                task_proto_dict.update({'ddm_back_end': 'rucio'})
                task_proto_dict.update({'prod_source': 'rucio_test'})

            if no_input and number_of_events > 0:
                task_proto_dict.update({'number_of_events': number_of_events})
            elif not no_input and number_of_events > 0:
                if prod_step.lower() != 'evgen'.lower() and 'nEventsPerInputFile' in task_config.keys():
                    number_input_files_requested = number_of_events / int(task_config['nEventsPerInputFile'])
                    if number_input_files_requested == 0:
                        raise Exception(
                            "Number of requested input files is null (Input events=%d, nEventsPerInputFile=%d)" %
                            (int(number_of_events), int(task_config['nEventsPerInputFile']))
                        )
                    task_proto_dict.update({'number_of_files': number_input_files_requested})
                elif prod_step.lower() != 'evgen'.lower() and not 'nEventsPerInputFile' in task_config.keys():
                    task_proto_dict.update({'number_of_events': number_of_events})

            if no_input:
                task_proto_dict.update({'no_primary_input': no_input})
                if not 'number_of_events' in task_proto_dict.keys():
                    raise Exception("Number of events to be processed is mandatory when task has no input")

            if no_input and not prod_step.lower() in ['evgen', 'simul']:
                raise Exception('This type of task ({0}) cannot be submitted without input'.format(prod_step))

            if 'nFiles' in task_config.keys():
                number_of_files = int(task_config['nFiles'])
                task_proto_dict.update({'number_of_files': number_of_files})

            if 'nEvents' in task_config.keys():
                task_proto_dict.update({'number_of_events': int(task_config['nEvents'])})

            if 'nEventsPerInputFile' in task_config.keys() and not no_input:
                number_of_events_per_input_file = int(task_config['nEventsPerInputFile'])
                task_proto_dict.update({'number_of_events_per_input_file': number_of_events_per_input_file})

            if 'nEventsPerJob' in task_config.keys():
                number_of_events_per_job = int(task_config['nEventsPerJob'])
                task_proto_dict.update({'number_of_events_per_job': number_of_events_per_job})

            if 'nFilesPerJob' in task_config.keys():
                number_of_files_per_job = int(task_config['nFilesPerJob'])
                if number_of_files_per_job == 0:
                    task_proto_dict.update({'number_of_files_per_job': None})
                else:
                    task_proto_dict.update({'number_of_files_per_job': number_of_files_per_job})
                if number_of_files_per_job > TaskDefConstants.DEFAULT_MAX_FILES_PER_JOB:
                    task_proto_dict.update({'number_of_max_files_per_job': number_of_files_per_job})

            if 'nEventsPerRange' in task_config.keys():
                number_of_events_per_range = int(task_config['nEventsPerRange'])
                task_proto_dict.update({'number_of_events_per_range': number_of_events_per_range})

            if 'nEventsPerInputFile' in task_config.keys() and 'nEventsPerJob' in task_config.keys():
                number_of_max_files_per_job = \
                    int(task_config['nEventsPerJob']) / int(task_config['nEventsPerInputFile'])
                if number_of_max_files_per_job > TaskDefConstants.DEFAULT_MAX_FILES_PER_JOB:
                    task_proto_dict.update({'number_of_max_files_per_job': number_of_max_files_per_job})

            if 'nGBPerJob' in task_config.keys():
                number_of_gb_per_job = int(task_config['nGBPerJob'])
                task_proto_dict.update({'number_of_gb_per_job': number_of_gb_per_job})

            if 'maxAttempt' in task_config.keys():
                max_attempt = int(task_config['maxAttempt'])
                task_proto_dict.update({'max_attempt': max_attempt})

            if 'maxFailure' in task_config.keys():
                max_failure = int(task_config['maxFailure'])
                task_proto_dict.update({'max_failure': max_failure})

            if 'nEventsPerMergeJob' in task_config.keys():
                number_of_events_per_merge_job = int(task_config['nEventsPerMergeJob'])
                task_proto_dict.update({'number_of_events_per_merge_job': number_of_events_per_merge_job})

            if step.request.phys_group.lower() in [e.lower() for e in ['THLT', 'REPR']]:
                task_proto_dict.update({'no_throttle': True})

            if step.request.phys_group.lower() in [e.lower() for e in ['REPR']]:
                task_proto_dict.update({'use_exhausted': True})

            if step.request.request_type.lower() == 'EVENTINDEX'.lower():
                task_proto_dict.update(({'ip_connectivity': "'full'"}))

            if 'ipConnectivity'.lower() in project_mode.keys():
                task_proto_dict.update({'ip_connectivity': "'%s'" % str(project_mode['ipConnectivity'.lower()])})

            if 'tgtNumEventsPerJob'.lower() in project_mode.keys():
                task_proto_dict.update({'tgt_num_events_per_job': int(project_mode['tgtNumEventsPerJob'.lower()])})

            if 'cpuTime'.lower() in project_mode.keys():
                task_proto_dict.update({'cpu_time': int(project_mode['cpuTime'.lower()])})

            if 'cpuTimeUnit'.lower() in project_mode.keys():
                task_proto_dict.update({'cpu_time_unit': str(project_mode['cpuTimeUnit'.lower()])})

            if 'workDiskCount'.lower() in project_mode.keys():
                task_proto_dict.update({'work_disk_count': int(project_mode['workDiskCount'.lower()])})

            if 'workDiskUnit'.lower() in project_mode.keys():
                task_proto_dict.update({'work_disk_unit': str(project_mode['workDiskUnit'.lower()])})

            if 'goal'.lower() in project_mode.keys():
                task_proto_dict.update({'goal': str(project_mode['goal'.lower()])})

            if 'skipFilesUsedBy'.lower() in project_mode.keys():
                task_proto_dict.update({'skip_files_used_by': int(project_mode['skipFilesUsedBy'.lower()])})
                skip_check_input = True

            if 'noThrottle'.lower() in project_mode.keys():
                option_value = str(project_mode['noThrottle'.lower()])
                if option_value.lower() == 'yes'.lower():
                    task_proto_dict.update({'no_throttle': True})
                elif option_value.lower() == 'no'.lower():
                    task_proto_dict.update({'no_throttle': None})

            if 'ramUnit'.lower() in project_mode.keys():
                task_proto_dict.update({'ram_unit': str(project_mode['ramUnit'.lower()])})

            if 'baseRamCount'.lower() in project_mode.keys():
                task_proto_dict.update({'base_ram_count': int(project_mode['baseRamCount'.lower()])})

            if 'nucleus'.lower() in project_mode.keys():
                task_proto_dict.update({'nucleus': str(project_mode['nucleus'.lower()])})

            if 'workQueueName'.lower() in project_mode.keys():
                task_proto_dict.update({'work_queue_name': str(project_mode['workQueueName'.lower()])})

            if 'allowInputWAN'.lower() in project_mode.keys():
                option_value = str(project_mode['allowInputWAN'.lower()])
                if option_value.lower() == 'yes'.lower():
                    task_proto_dict.update({'allow_input_wan': True})
                elif option_value.lower() == 'no'.lower():
                    task_proto_dict.update({'allow_input_wan': None})

            if 'allowInputLAN'.lower() in project_mode.keys():
                task_proto_dict.update({'allow_input_lan': "'{0}'".format(str(project_mode['allowInputLAN'.lower()]))})

            if 'nMaxFilesPerJob'.lower() in project_mode.keys():
                task_proto_dict.update({'number_of_max_files_per_job': int(project_mode['nMaxFilesPerJob'.lower()])})

            try:
                ttcr_timestamp = None
                ttcr = TConfig.get_ttcr(project, prod_step, usergroup)
                if ttcr > 0:
                    ttcr_timestamp = timezone.now() + datetime.timedelta(seconds=ttcr)
                    task_proto_dict.update({'ttcr_timestamp': str(ttcr_timestamp)})
            except Exception as ex:
                logger.exception('Getting TTC failed: {0}'.format(str(ex)))

            if 'useJobCloning'.lower() in project_mode.keys():
                task_proto_dict.update({'use_job_cloning': str(project_mode['useJobCloning'.lower()])})

            if 'nSitesPerJob'.lower() in project_mode.keys():
                task_proto_dict.update({'number_of_sites_per_job': int(project_mode['nSitesPerJob'.lower()])})

            if 'altStageOut'.lower() in project_mode.keys():
                task_proto_dict.update({'alt_stage_out': str(project_mode['altStageOut'.lower()])})

            if 'cpuEfficiency'.lower() in project_mode.keys():
                task_proto_dict.update({'cpu_efficiency': int(project_mode['cpuEfficiency'.lower()])})

            if 'minGranularity'.lower() in project_mode.keys():
                task_proto_dict.update({'min_granularity': int(project_mode['minGranularity'.lower()])})

            if 'respectSplitRule'.lower() in project_mode.keys():
                option_value = str(project_mode['respectSplitRule'.lower()])
                if option_value.lower() == 'yes'.lower():
                    task_proto_dict.update({'respect_split_rule': True})
                elif option_value.lower() == 'no'.lower():
                    task_proto_dict.update({'respect_split_rule': None})

            if step.request.request_type.lower() == 'MC'.lower():
                if prod_step.lower() == 'simul'.lower() and int(trf_release.split('.')[0]) >= 21:
                    if not 'esConvertible'.lower() in project_mode.keys():
                        project_mode['esConvertible'.lower()] = 'yes'

            if 'esFraction'.lower() in project_mode.keys():
                es_fraction = float(project_mode['esFraction'.lower()])
                if es_fraction > 0:
                    task_proto_dict.update({'es_fraction': es_fraction})
                    task_proto_dict.update({'es_convertible': True})
                    project_mode['esMerging'.lower()] = 'yes'
                    # task_proto_dict['max_attempt_es'] = TaskDefConstants.DEFAULT_ES_MAX_ATTEMPT
                    # task_proto_dict['max_attempt_es_job'] = TaskDefConstants.DEFAULT_ES_MAX_ATTEMPT_JOB

            if 'esConvertible'.lower() in project_mode.keys():
                option_value = str(project_mode['esConvertible'.lower()])
                if option_value.lower() == 'yes'.lower():
                    task_proto_dict.update({'es_convertible': True})
                    project_mode['esMerging'.lower()] = 'yes'
                    # task_proto_dict['max_attempt_es'] = TaskDefConstants.DEFAULT_ES_MAX_ATTEMPT
                    # task_proto_dict['max_attempt_es_job'] = TaskDefConstants.DEFAULT_ES_MAX_ATTEMPT_JOB
                    task_proto_dict['not_discard_events'] = True
                elif option_value.lower() == 'no'.lower():
                    task_proto_dict.update({'es_convertible': None})

            if 'esMerging'.lower() in project_mode.keys():
                option_value = str(project_mode['esMerging'.lower()])
                if option_value.lower() == 'yes'.lower():
                    es_merging_tag_name = ctag_name
                    es_merging_trf_name = 'HITSMerge_tf.py'
                    # es_merging_tag_name = str(project_mode['esMerging'.lower()])
                    # es_merging_tag = self._get_ami_tag_cached(es_merging_tag_name)
                    # es_merging_trf_name = es_merging_tag['transformation']
                    # if es_merging_trf_name.lower() != 'HITSMerge_tf.py'.lower():
                    #     raise Exception(
                    #         'Only HITSMerge_tf.py is allowed for ES merging. But \"{0}\" was provided (AMI tag: {1})'.format(
                    #             es_merging_trf_name, es_merging_tag_name))
                    task_proto_dict['es_merge_spec'] = {}
                    task_proto_dict['es_merge_spec']['transPath'] = es_merging_trf_name
                    name_postfix = ''
                    if trf_release in ['20.3.7.5', '20.7.8.7']:
                        name_postfix = '_000'
                    task_proto_dict['es_merge_spec']['jobParameters'] = \
                        '--AMITag {0} --DBRelease=current --autoConfiguration=everything '.format(es_merging_tag_name) + \
                        '--outputHitsFile=${OUTPUT0} --inputHitsFile=@inputFor_${OUTPUT0}' + name_postfix

            if 'esConsumers'.lower() in project_mode.keys():
                task_proto_dict['number_of_es_consumers'] = int(project_mode['esConsumers'.lower()])

            if 'esMaxAttempt'.lower() in project_mode.keys():
                task_proto_dict['max_attempt_es'] = int(project_mode['esMaxAttempt'.lower()])

            if 'esMaxAttemptJob'.lower() in project_mode.keys():
                task_proto_dict['max_attempt_es_job'] = int(project_mode['esMaxAttemptJob'.lower()])

            if 'nJumboJobs'.lower() in project_mode.keys():
                task_proto_dict['number_of_jumbo_jobs'] = int(project_mode['nJumboJobs'.lower()])

            if 'nEventsPerWorker'.lower() in project_mode.keys():
                task_proto_dict['number_of_events_per_worker'] = int(project_mode['nEventsPerWorker'.lower()])

            if 'processingType'.lower() in project_mode.keys():
                task_proto_dict.update({'type': str(project_mode['processingType'.lower()])})

            if 'prodSourceLabel'.lower() in project_mode.keys():
                task_proto_dict.update({'prod_source': str(project_mode['prodSourceLabel'.lower()])})

            if 'skipShortInput'.lower() in project_mode.keys():
                option_value = str(project_mode['skipShortInput'.lower()])
                if option_value.lower() == 'yes'.lower():
                    task_proto_dict.update({'skip_short_input': True})
                elif option_value.lower() == 'no'.lower():
                    task_proto_dict.update({'skip_short_input': None})

            if 'registerEsFiles'.lower() in project_mode.keys():
                option_value = str(project_mode['registerEsFiles'.lower()])
                if option_value.lower() == 'yes'.lower():
                    task_proto_dict.update({'register_es_files': True})
                elif option_value.lower() == 'no'.lower():
                    task_proto_dict.update({'register_es_files': None})

            if 'transUsesPrefix'.lower() in project_mode.keys():
                trans_uses_prefix = str(project_mode['transUsesPrefix'.lower()])
                if trans_uses_prefix:
                    task_proto_dict.update({'trans_uses_prefix': trans_uses_prefix})

            if 'noWaitParent'.lower() in project_mode.keys():
                option_value = str(project_mode['noWaitParent'.lower()])
                if option_value.lower() == 'yes'.lower():
                    task_proto_dict.update({'no_wait_parent': True})
                elif option_value.lower() == 'no'.lower():
                    task_proto_dict.update({'no_wait_parent': None})

            if 'usePrefetcher'.lower() in project_mode.keys():
                option_value = str(project_mode['usePrefetcher'.lower()])
                if option_value.lower() == 'yes'.lower():
                    task_proto_dict.update({'use_prefetcher': True})
                elif option_value.lower() == 'no'.lower():
                    task_proto_dict.update({'use_prefetcher': None})

            if 'disableAutoFinish'.lower() in project_mode.keys():
                option_value = str(project_mode['disableAutoFinish'.lower()])
                if option_value.lower() == 'yes'.lower():
                    task_proto_dict.update({'disable_auto_finish': True})
                elif option_value.lower() == 'no'.lower():
                    task_proto_dict.update({'disable_auto_finish': None})

            if 'isMergeTask'.lower() in project_mode.keys():
                option_value = str(project_mode['isMergeTask'.lower()])
                if option_value.lower() == 'yes'.lower():
                    task_proto_dict.update({'use_exhausted': True})
                    task_proto_dict.update({'goal': str(100.0)})
                    task_proto_dict.update({'fail_when_goal_unreached': False})
                    task_proto_dict.update({'disable_auto_finish': True})

            if 'outDiskCount'.lower() in project_mode.keys():
                task_proto_dict['out_disk_count'] = int(project_mode['outDiskCount'.lower()])
                task_proto_dict['out_disk_unit'] = 'kB'

            if 'inFilePosEvtNum'.lower() in project_mode.keys():
                option_value = str(project_mode['inFilePosEvtNum'.lower()])
                if option_value.lower() == 'yes'.lower():
                    task_proto_dict.update({'in_file_pos_evt_num': True})
                elif option_value.lower() == 'no'.lower():
                    task_proto_dict.update({'in_file_pos_evt_num': None})

            if 'tgtMaxOutputForNG'.lower() in project_mode.keys():
                task_proto_dict.update({'tgt_max_output_for_ng': int(project_mode['tgtMaxOutputForNG'.lower()])})
            if 'maxWalltime'.lower() in project_mode.keys():
                task_proto_dict.update({'max_walltime': int(project_mode['maxWalltime'.lower()])})

            if 'notDiscardEvents'.lower() in project_mode.keys():
                option_value = str(project_mode['notDiscardEvents'.lower()])
                if option_value.lower() == 'yes'.lower():
                    task_proto_dict.update({'not_discard_events': True})
                elif option_value.lower() == 'no'.lower():
                    task_proto_dict.update({'not_discard_events': None})

            if 'scoutSuccessRate'.lower() in project_mode.keys():
                task_proto_dict.update({'scout_success_rate': int(project_mode['scoutSuccessRate'.lower()])})

            reuse_input = None
            if 'reuseInput'.lower() in project_mode.keys():
                option_value = int(project_mode['reuseInput'.lower()])
                if option_value > 0:
                    reuse_input = option_value

            if 'orderByLB'.lower() in project_mode.keys():
                option_value = str(project_mode['orderByLB'.lower()])
                if option_value.lower() == 'yes'.lower():
                    task_proto_dict.update({'order_by_lb': True})
                elif option_value.lower() == 'no'.lower():
                    task_proto_dict.update({'order_by_lb': None})

            truncate_output_formats = None
            if 'truncateOutputFormats'.lower() in project_mode.keys():
                option_value = str(project_mode['truncateOutputFormats'.lower()])
                if option_value.lower() == 'yes'.lower():
                    truncate_output_formats = True
                elif option_value.lower() == 'no'.lower():
                    truncate_output_formats = False

            if 'useZipToPin'.lower() in project_mode.keys():
                option_value = str(project_mode['useZipToPin'.lower()])
                if option_value.lower() == 'yes'.lower():
                    task_proto_dict.update({'use_zip_to_pin': True})
                elif option_value.lower() == 'no'.lower():
                    task_proto_dict.update({'use_zip_to_pin': None})

            # FIXME
            # task_proto_dict.update({'put_log_to_os': True})

            # FIXME
            # if 'nEventsPerJob'.lower() in project_mode.keys():
            #     task_proto_dict.update({'number_of_events_per_job': int(project_mode['nEventsPerJob'.lower()])})
            if 'nEventsPerInputFile'.lower() in project_mode.keys():
                number_of_events_per_input_file = int(project_mode['nEventsPerInputFile'.lower()])
                task_proto_dict.update({'number_of_events_per_input_file': number_of_events_per_input_file})
            # if 'nEventsPerRange'.lower() in project_mode.keys():
            #     number_of_events_per_range = int(project_mode['nEventsPerRange'.lower()])
            #     task_proto_dict.update({'number_of_events_per_range': number_of_events_per_range})
            # if 'nEventsPerMergeJob'.lower() in project_mode.keys():
            #     task_proto_dict.update(
            #         {'number_of_events_per_merge_job': int(project_mode['nEventsPerMergeJob'.lower()])}
            #     )

            # FIXME
            # if prod_step.lower() == 'simul'.lower():
            #     if not 'cpuTime'.lower() in project_mode.keys():
            #         task_proto_dict.update({'cpu_time': 3000})
            #     if not 'cpuTimeUnit'.lower() in project_mode.keys():
            #         task_proto_dict.update({'cpu_time_unit': 'HS06sPerEvent'})

            if step.request.request_type.lower() == 'MC'.lower():
                if 'nEventsPerJob' in task_config.keys() and number_of_events > 0:
                    number_of_jobs = int(number_of_events) / int(task_config['nEventsPerJob'])
                    if number_of_jobs <= 10:
                        task_proto_dict.update({'use_exhausted': True})
                        task_proto_dict.update({'goal': str(100.0)})
                        task_proto_dict.update({'fail_when_goal_unreached': False})
                        task_proto_dict.update({'disable_auto_finish': True})
                        # if number_of_events <= 1000:
                        #     task_proto_dict.update({'use_exhausted': True})
                        #     task_proto_dict.update({'goal': str(100.0)})
                        #     task_proto_dict.update({'fail_when_goal_unreached': True})
                        # elif number_of_events > 1000:
                        #     task_proto_dict.update({'use_exhausted': True})
                        #     task_proto_dict.update({'goal': str(90.0)})
                    else:
                        if number_of_events <= 1000:
                            task_proto_dict.update({'use_exhausted': True})
                            task_proto_dict.update({'goal': str(100.0)})
                            task_proto_dict.update({'fail_when_goal_unreached': True})
                # if number_of_events > 0:
                #     small_events_numbers = dict()
                #     small_events_numbers.update({
                #         r'mc15_13TeV': 10000,
                #         r'mc16_13TeV': 2000,
                #         r'mc15:mc15(a|b|c)': 10000,
                #         r'mc16:mc16(a|b|c|\*)': 2000
                #     })
                #     small_events_threshold = 0
                #     for pattern in small_events_numbers.keys():
                #         if re.match(pattern, project, re.IGNORECASE) or re.match(pattern, campaign, re.IGNORECASE):
                #             small_events_threshold = small_events_numbers[pattern]
                #             break
                #     if number_of_events < small_events_threshold:
                #         force_small_events = False
                #         if 'isSmallEvents'.lower() in project_mode.keys():
                #             option_value = str(project_mode['isSmallEvents'.lower()])
                #             if option_value.lower() == 'yes'.lower():
                #                 force_small_events = True
                #         if not force_small_events:
                #             raise TaskSmallEventsException(number_of_events)
            if not evgen_params:
                self._check_number_of_events(step, project_mode)

            if number_of_events > 0 and 'nEventsPerJob' in task_config.keys():
                number_of_jobs = number_of_events / int(task_config['nEventsPerJob'])
                if number_of_jobs > TaskDefConstants.DEFAULT_MAX_NUMBER_OF_JOBS_PER_TASK:
                    raise MaxJobsPerTaskLimitExceededException(number_of_jobs)

            if 'failWhenGoalUnreached'.lower() in project_mode.keys():
                option_value = str(project_mode['failWhenGoalUnreached'.lower()])
                if option_value.lower() == 'yes'.lower():
                    task_proto_dict.update({'fail_when_goal_unreached': True})
                elif option_value.lower() == 'no'.lower():
                    task_proto_dict.update({'fail_when_goal_unreached': None})

            io_intensity = None

            if step.request.phys_group.lower() in [e.lower() for e in ['REPR']]:
                if trf_name.lower() == 'ESDMerge_tf.py'.lower():
                    io_intensity = 3000
                elif trf_name.lower() == 'AODMerge_tf.py'.lower():
                    io_intensity = 400
                elif trf_name.lower() == 'HISTMerge_tf.py'.lower():
                    io_intensity = 3000
                elif trf_name.lower() == 'DAODMerge_tf'.lower():
                    io_intensity = 400
                elif trf_name.lower() == 'HISTMerge_tf.py'.lower():
                    io_intensity = 2000

            if io_intensity:
                task_proto_dict.update({'io_intensity': int(io_intensity)})
                task_proto_dict.update({'io_intensity_unit': 'kBPerS'})

            # number_of_max_files_per_job = 20
            # task_proto_dict.update({'number_of_max_files_per_job': 50})

            # # test JEDI merging: impossible to use --maxEvents=${MAXEVENTS}
            # task_proto_dict['merge_output'] = True
            # task_proto_dict['merge_spec'] = {}
            # task_proto_dict['merge_spec']['transPath'] = "HITSMerge_tf.py"
            # task_proto_dict['merge_spec']['jobParameters'] = \
            # "--AMITag=s1776 " \
            # "--autoConfiguration=everything " \
            # "--DBRelease=current " \
            # "--postInclude=RecJobTransforms/UseFrontierFallbackDBRelease.py " \
            # "--outputHits_MRGFile=${OUTPUT0} " \
            # "--inputHitsFile=${TRN_OUTPUT0} " \
            # "--inputLogsFile=${TRN_LOG0}"

            # test JEDI merging for train production
            # if train_production:
            # task_proto_dict['um_name_at_end'] = True
            # task_proto_dict['merge_output'] = True
            # task_proto_dict['merge_spec'] = {}
            # task_proto_dict['merge_spec']['transPath'] = "AODMerge_tf.py"
            # task_proto_dict['merge_spec']['jobParameters'] = \
            # "--AMITag=p1787 " \
            # "--autoConfiguration=everything " \
            # "--fastPoolMerge=False " \
            # "--postExec=GlobalEventTagBuilder.Enable=False " \
            #     "--outputAOD_MRGFile=${OUTPUT0} " \
            #     "--inputAODFile=${TRN_OUTPUT0} "
            # task_proto_dict['um_name_at_end'] = True
            # task_proto_dict['merge_output'] = True
            # task_proto_dict['merge_spec'] = {}
            # task_proto_dict['merge_spec']['transPath'] = "DAODMerge_tf.py"
            # task_proto_dict['merge_spec']['jobParameters'] = \
            #     "--autoConfiguration=everything " \
            #     "--outputDAOD_EGAM1_MRGFile=${OUTPUT0} " \
            #     "--inputDAOD_EGAM1File=${TRN_OUTPUT0} " \
            #     "--outputDAOD_EGAM3_MRGFile=${OUTPUT1} " \
            #     "--inputDAOD_EGAM3File=${TRN_OUTPUT1} "

            # test Event Service
            if 'testES'.lower() in project_mode.keys():
                option_value = str(project_mode['testES'.lower()])
                if option_value.lower() == 'yes'.lower():
                    # skip_check_input = True
                    # task_proto_dict['prod_source'] = 'ptest'
                    if 'nEventsPerWorker'.lower() in project_mode.keys():
                        task_proto_dict['number_of_events_per_worker'] = int(project_mode['nEventsPerWorker'.lower()])
                    else:
                        task_proto_dict['number_of_events_per_worker'] = 1
                    if 'nEsConsumers'.lower() in project_mode.keys():
                        task_proto_dict['number_of_es_consumers'] = int(project_mode['nEsConsumers'.lower()])
                    else:
                        task_proto_dict['number_of_es_consumers'] = 1
                    if 'esProcessingType'.lower() in project_mode.keys():
                        task_proto_dict['type'] = str(project_mode['esProcessingType'.lower()])
                    # else:
                    #     task_proto_dict['type'] = 'validation'
                    if 'maxAttemptES'.lower() in project_mode.keys():
                        task_proto_dict['max_attempt_es'] = int(project_mode['maxAttemptES'.lower()])
                    task_proto_dict['es_merge_spec'] = {}
                    task_proto_dict['es_merge_spec']['transPath'] = 'HITSMerge_tf.py'
                    name_postfix = ''
                    if trf_release in ['20.3.7.5', '20.7.8.7']:
                        name_postfix = "_000"
                    task_proto_dict['es_merge_spec']['jobParameters'] = \
                        "--AMITag s2049 --DBRelease=current --autoConfiguration=everything " \
                        "--outputHitsFile=${OUTPUT0} --inputHitsFile=@inputFor_${OUTPUT0}" + name_postfix
                    # "--postInclude=RecJobTransforms/UseFrontierFallbackDBRelease.py " \

            if not 'number_of_events_per_input_file' in task_proto_dict.keys() and \
                    not 'number_of_gb_per_job' in task_proto_dict.keys():
                if not 'number_of_files_per_job' in task_proto_dict.keys():
                    task_proto_dict.update({'number_of_files_per_job': 1})

            # if use_real_nevents and 'tgt_num_events_per_job' in task_proto_dict.keys():
            if use_real_nevents:
                task_proto_dict.update({'number_of_files_per_job': None})
                if not 'number_of_max_files_per_job' in task_proto_dict.keys():
                    task_proto_dict.update({'number_of_max_files_per_job': 200})

            # new randomSeed format
            # if 'number_of_events_per_job' in task_proto_dict.keys() and number_of_events > 0:
            #     number_of_requested_jobs = int(number_of_events) / int(task_proto_dict['number_of_events_per_job'])
            #     random_seed_param = self._get_job_parameter('randomSeed', job_parameters)
            #     random_seed_param['num_records'] = number_of_requested_jobs

            if 'number_of_gb_per_job' in task_proto_dict.keys():
                if not 'nMaxFilesPerJob'.lower() in project_mode.keys():
                    task_proto_dict.update({'number_of_max_files_per_job': 1000})

            if use_real_nevents and 'number_of_events_per_input_file' in task_proto_dict.keys():
                raise TaskConfigurationException(
                    "The task is rejected due to incompatible parameters: useRealNumEvents, 'Events per Input file'"
                )

            self._define_merge_params(step, task_proto_dict, train_production)

            task_proto = self.protocol.render_task(task_proto_dict)

            task_elements = list()

            input_file_dict = dict()
            for key in input_params.keys():
                if re.match(r'^(--)?input.*File$', key, re.IGNORECASE):
                    input_file_dict.update({key: input_params[key]})

            if len(input_file_dict.keys()):
                input_list_length = len(input_file_dict[input_file_dict.keys()[0]])
                all_lists = [input_file_dict[key] for key in input_file_dict.keys()]
                if any(len(input_list) != input_list_length for input_list in all_lists):
                    raise Exception("Input lists are different lengths")
                context_dict_list = list()
                for i in range(input_list_length):
                    context_dict = dict()
                    for key in input_file_dict.keys():
                        context_dict.update({"%s_dataset" % key: input_file_dict[key][i]})
                    if len(context_dict.keys()):
                        context_dict_list.append(context_dict)
                for context_dict in context_dict_list:
                    template_string = self.protocol.serialize_task(task_proto)
                    task_template = Template(template_string)
                    task_string = task_template.render(Context(context_dict))
                    task_id = self.task_reg.register_task_id()
                    # FIXME
                    task_string = task_string.replace(TaskDefConstants.DEFAULT_TASK_ID_FORMAT % task_proto_id,
                                                      TaskDefConstants.DEFAULT_TASK_ID_FORMAT % task_id)

                    task = self.protocol.deserialize_task(task_string)
                    task_elements.append({task_id: task})
            else:
                task_string = self.protocol.serialize_task(task_proto)
                task_id = self.task_reg.register_task_id()
                # FIXME
                task_string = task_string.replace(TaskDefConstants.DEFAULT_TASK_ID_FORMAT % task_proto_id,
                                                  TaskDefConstants.DEFAULT_TASK_ID_FORMAT % task_id)
                task = self.protocol.deserialize_task(task_string)
                task_elements.append({task_id: task})

            if not len(task_elements):
                raise Exception("List of tasks is empty")

            for task_element in task_elements:
                task_id = task_element.keys()[0]
                task = task_element.values()[0]

                for key in output_params.keys():
                    for output_dataset_name in output_params[key]:
                        output_dataset_name = output_dataset_name.replace(
                            TaskDefConstants.DEFAULT_TASK_ID_FORMAT % task_proto_id,
                            TaskDefConstants.DEFAULT_TASK_ID_FORMAT % task_id)
                        if len(output_dataset_name) > TaskDefConstants.DEFAULT_OUTPUT_NAME_MAX_LENGTH:
                            raise OutputNameMaxLengthException(output_dataset_name)

                if step.request.request_type.lower() == 'MC'.lower():
                    if prod_step.lower() == 'simul'.lower() and int(trf_release.split('.')[0]) >= 21:
                        self._check_task_number_of_jobs(task, number_of_events, step)

                self._check_task_unmerged_input(task, step, prod_step)
                self._check_task_merged_input(task, step, prod_step)
                # self._check_task_cache_version_consistency(task, step, prod_step, trf_release)

                # FIXME
                if not skip_check_input:
                    self._check_task_input(task, task_id, number_of_events, task_config, parent_task_id,
                                           input_data_name, step, primary_input_offset, prod_step,
                                           reuse_input=reuse_input, evgen_params=evgen_params,
                                           task_common_offset=task_common_offset)

                if step == first_step:
                    chain_id = task_id
                    primary_input_offset = 0
                    if first_parent_task_id:
                        parent_task_id = first_parent_task_id
                    else:
                        parent_task_id = self.task_reg.get_parent_task_id(step, task_id)

                # primary_input = self._get_primary_input(task['jobParameters'])
                # if primary_input:
                #     input_data_name = primary_input['dataset'].split(':')[-1]

                self.task_reg.register_task(task, step, task_id, parent_task_id, chain_id, project, input_data_name,
                                            number_of_events, step.request.campaign, step.request.subcampaign,
                                            bunchspacing, ttcr_timestamp,
                                            truncate_output_formats=truncate_output_formats,
                                            task_common_offset=task_common_offset)

                self.task_reg.register_task_output(output_params,
                                                   task_proto_id,
                                                   task_id,
                                                   parent_task_id,
                                                   usergroup,
                                                   step.request.subcampaign)

                parent_task_id = task_id

    def _get_number_events_processed(self, step, requested_datasets=None):
        number_events_processed = 0
        input_data_name = self.get_step_input_data_name(step)

        ps1_task_list = TTaskRequest.objects.filter(~Q(status__in=['failed', 'broken', 'aborted', 'obsolete']),
                                                    project=step.request.project,
                                                    inputdataset=input_data_name,
                                                    ctag=step.step_template.ctag,
                                                    formats=step.step_template.output_formats)
        for ps1_task in ps1_task_list:
            number_events_processed += int(ps1_task.total_events or 0)

        split_slice = self._get_task_config(step).get('split_slice')

        if split_slice:
            ps2_task_list = \
                ProductionTask.objects.filter(~Q(status__in=['failed', 'broken', 'aborted', 'obsolete', 'toabort']) &
                                              (Q(step__slice__input_dataset=input_data_name) |
                                               Q(step__slice__input_dataset__endswith=input_data_name.split(':')[-1]) |
                                               Q(step__slice__input_data=input_data_name) |
                                               Q(step__slice__input_data__endswith=input_data_name.split(':')[-1])),
                                              project=step.request.project,
                                              step__step_template__ctag=step.step_template.ctag)
        else:
            ps2_task_list = \
                ProductionTask.objects.filter(~Q(status__in=['failed', 'broken', 'aborted', 'obsolete', 'toabort']) &
                                              (Q(step__slice__input_dataset=input_data_name) |
                                               Q(step__slice__input_dataset__endswith=input_data_name.split(':')[-1]) |
                                               Q(step__slice__input_data=input_data_name) |
                                               Q(step__slice__input_data__endswith=input_data_name.split(':')[-1])),
                                              project=step.request.project,
                                              step__step_template__ctag=step.step_template.ctag,
                                              step__step_template__output_formats=step.step_template.output_formats)

        for ps2_task in ps2_task_list:

            if split_slice:
                # comparing output formats
                requested_output_types = step.step_template.output_formats.split('.')
                previous_output_types = ps2_task.step.step_template.output_formats.split('.')
                processed_output_types = [e for e in requested_output_types if e in previous_output_types]
                if not processed_output_types:
                    continue

            if requested_datasets:
                jedi_task_existing = TTask.objects.get(id=ps2_task.id)
                task_existing = json.loads(jedi_task_existing.jedi_task_param)
                previous_dsn = self._get_primary_input(task_existing['jobParameters'])['dataset']
                requested_datasets_no_scope = [e.split(':')[-1] for e in requested_datasets]
                previous_dsn_no_scope = previous_dsn.split(':')[-1]
                if not previous_dsn_no_scope in requested_datasets_no_scope:
                    continue

            number_events = int(ps2_task.total_req_events or 0)
            if not number_events:
                number_events = int(ps2_task.total_events or 0)
            number_events_processed += number_events

        return number_events_processed

    def _get_processed_datasets(self, step, requested_datasets=None):
        processed_datasets = []
        input_data_name = self.get_step_input_data_name(step)
        # Drop ps1
        # ps1_task_list = TTaskRequest.objects.filter(~Q(status__in=['failed', 'broken', 'aborted', 'obsolete']),
        #                                             project=step.request.project,
        #                                             inputdataset=input_data_name,
        #                                             ctag=step.step_template.ctag,
        #                                             formats=step.step_template.output_formats)
        # for ps1_task in ps1_task_list:
        #     number_events_processed += int(ps1_task.total_events or 0)

        split_slice = self._get_task_config(step).get('split_slice')

        if split_slice:
            ps2_task_list = \
                ProductionTask.objects.filter(~Q(status__in=['failed', 'broken', 'aborted', 'obsolete', 'toabort']) &
                                              (Q(step__slice__input_dataset=input_data_name) |
                                               Q(step__slice__input_dataset__endswith=input_data_name.split(':')[-1]) |
                                               Q(step__slice__input_data=input_data_name) |
                                               Q(step__slice__input_data__endswith=input_data_name.split(':')[-1])),
                                              project=step.request.project,
                                              step__step_template__ctag=step.step_template.ctag)
        else:
            ps2_task_list = \
                ProductionTask.objects.filter(~Q(status__in=['failed', 'broken', 'aborted', 'obsolete', 'toabort']) &
                                              (Q(step__slice__input_dataset=input_data_name) |
                                               Q(step__slice__input_dataset__endswith=input_data_name.split(':')[-1]) |
                                               Q(step__slice__input_data=input_data_name) |
                                               Q(step__slice__input_data__endswith=input_data_name.split(':')[-1])),
                                              project=step.request.project,
                                              step__step_template__ctag=step.step_template.ctag,
                                              step__step_template__output_formats=step.step_template.output_formats)

        for ps2_task in ps2_task_list:

            if split_slice:
                # comparing output formats
                requested_output_types = step.step_template.output_formats.split('.')
                previous_output_types = ps2_task.step.step_template.output_formats.split('.')
                processed_output_types = [e for e in requested_output_types if e in previous_output_types]
                if not processed_output_types:
                    continue
            jedi_task_existing = TTask.objects.get(id=ps2_task.id)
            task_existing = json.loads(jedi_task_existing.jedi_task_param)
            previous_dsn = self._get_primary_input(task_existing['jobParameters'])['dataset']
            requested_datasets_no_scope = [e.split(':')[-1] for e in requested_datasets]
            previous_dsn_no_scope = previous_dsn.split(':')[-1]
            if requested_datasets:
                if not previous_dsn_no_scope in requested_datasets_no_scope:
                    continue
            processed_datasets.append(previous_dsn_no_scope)
        return processed_datasets

    def get_events_per_file(self, input_name):
        nevents_per_file = 0
        try:
            try:
                nevents_per_file = self.rucio_client.get_nevents_per_file(input_name)
            except:
                nevents_per_file = self.ami_client.get_nevents_per_file(input_name)
        except:
            logger.info("get_nevents_per_file, exception occurred: %s" % get_exception_string())
        return nevents_per_file

    def get_events_per_input_file(self, step, input_name, use_real_events=False):
        task_config = self._get_task_config(step)
        if not 'nEventsPerInputFile' in task_config.keys() or use_real_events:
            events_per_file = int(self.get_events_per_file(input_name))
        else:
            events_per_file = int(task_config['nEventsPerInputFile'])
        return events_per_file

    # def get_events_in_container(self, step, input_name, use_real_events=False, content=None):
    #     number_events_in_container = 0
    #     if not content:
    #         result = self.ddm_wrapper.get_datasets_and_containers(input_name, datasets_contained_only=True)
    #         for dataset_name in result['datasets']:
    #             events_per_file = self.get_events_per_input_file(step, dataset_name, use_real_events=use_real_events)
    #             number_events_in_dataset = events_per_file * self.ddm_wrapper.ddm_get_number_files(dataset_name)
    #             number_events_in_container += number_events_in_dataset
    #     else:
    #         for dataset_name in content:
    #             events_per_file = self.get_events_per_input_file(step, dataset_name, use_real_events=use_real_events)
    #             number_events_in_dataset = events_per_file * self.ddm_wrapper.ddm_get_number_files(dataset_name)
    #             number_events_in_container += number_events_in_dataset
    #     return number_events_in_container

    def get_events_in_datasets(self, datasets, step, use_real_events=False):
        number_events = 0
        for dataset_name in datasets:
            events_per_file = self.get_events_per_input_file(step, dataset_name, use_real_events=use_real_events)
            number_events_in_dataset = events_per_file * self.rucio_client.get_number_files(dataset_name)
            number_events += number_events_in_dataset
        return number_events

    def get_dataset_subcampaign(self, name):
        task_id = self._get_parent_task_id_from_input(name)
        if task_id == 0:
            return None

        tasks = ProductionTask.objects.filter(id=task_id)
        if not tasks:
            subcampaign = self.rucio_client.get_campaign(name)
            for e in TaskDefConstants.DEFAULT_SC_HASHTAGS.keys():
                for pattern in TaskDefConstants.DEFAULT_SC_HASHTAGS[e]:
                    result = re.match(r'{0}'.format(pattern), subcampaign)
                    if result:
                        return e

        task = tasks[0]
        sc_hashtags = \
            [e + TaskDefConstants.DEFAULT_SC_HASHTAG_SUFFIX for e in TaskDefConstants.DEFAULT_SC_HASHTAGS.keys()]
        for e in sc_hashtags:
            try:
                hashtag = HashTag.objects.get(hashtag=e)
            except ObjectDoesNotExist:
                hashtag = HashTag(hashtag=e, type='UD')
                hashtag.save()
            if task.hashtag_exists(hashtag):
                return e.split(TaskDefConstants.DEFAULT_SC_HASHTAG_SUFFIX)[0]

        subcampaign = self.rucio_client.get_campaign(name)
        for e in TaskDefConstants.DEFAULT_SC_HASHTAGS.keys():
            for pattern in TaskDefConstants.DEFAULT_SC_HASHTAGS[e]:
                result = re.match(r'{0}'.format(pattern), subcampaign)
                if result:
                    hashtag = HashTag.objects.get(hashtag=e + TaskDefConstants.DEFAULT_SC_HASHTAG_SUFFIX)
                    task.set_hashtag(hashtag)
                    return e

        return None

    def verify_container_consistency(self, input_name):
        if not self.rucio_client.is_dsn_container(input_name):
            return True

        dataset_list = list()
        result = self.rucio_client.get_datasets_and_containers(input_name, datasets_contained_only=True)
        dataset_list.extend(result['datasets'])

        previous_events_per_file = 0

        for dataset_name in dataset_list:
            number_files = self.rucio_client.get_number_files(dataset_name)
            number_events = self.rucio_client.get_number_events(dataset_name)
            round_up = lambda num: int(num + 1) if int(num) != num else int(num)
            events_per_file = round_up(float(number_events) / float(number_files))
            if previous_events_per_file == 0:
                previous_events_per_file = events_per_file
            else:
                if events_per_file != previous_events_per_file:
                    return False
        return True

    def verify_data_uniform(self, step, input_name):
        data_type = None
        try:
            data_type = self.parse_data_name(input_name)['data_type']
        except Exception:
            pass
        if data_type in ['TXT']:
            return

        task_config = self._get_task_config(step)
        project_mode = self._get_project_mode(step)
        config_events_per_file = int(task_config.get('nEventsPerInputFile', 0))
        if not config_events_per_file:
            return
        dataset_list = list()

        if self.rucio_client.is_dsn_container(input_name):
            result = self.rucio_client.get_datasets_and_containers(input_name, datasets_contained_only=True)
            dataset_list.extend(result['datasets'])
        else:
            dataset_list.append(input_name)

        for dataset_name in dataset_list:
            events_per_file = 0
            number_files = self.rucio_client.get_number_files(dataset_name)
            number_events = self.rucio_client.get_number_events(dataset_name)
            if number_events > 0:
                round_up = lambda num: int(num + 1) if int(num) != num else int(num)
                events_per_file = round_up(float(number_events) / float(number_files))

            # FIXME
            if not events_per_file:
                continue

            parent_events_per_job = 0
            parent_task_id = 0
            try:
                result = re.match(r'^.+_tid(?P<tid>\d+)_00$', dataset_name)
                if result:
                    parent_task = ProductionTask.objects.get(id=int(result.groupdict()['tid']))
                    parent_task_id = int(parent_task.id)
                    parent_events_per_job = int(self._get_task_config(parent_task.step).get('nEventsPerJob', 0))
            except Exception as ex:
                logger.exception('Getting parent nEventsPerJob failed: {0}'.format(str(ex)))

            if parent_events_per_job:
                if config_events_per_file != parent_events_per_job:
                    if 'nEventsPerInputFile'.lower() in project_mode.keys():
                        pass
                    else:
                        raise UniformDataException(dataset_name, events_per_file, number_events, number_files,
                                                   config_events_per_file, parent_events_per_job, parent_task_id)

    def _get_splitting_dict(self, step):
        # splitting chains
        splitting_dict = dict()
        if step.request.request_type.lower() in ['MC'.lower(), 'GROUP'.lower()]:
            ctag = self._get_ami_tag_cached(
                step.step_template.ctag)  # self.ami_wrapper.get_ami_tag(step.step_template.ctag)
            prod_step = self._get_prod_step(step.step_template.ctag,
                                            ctag)  # str(ctag['productionStep']).replace(' ', '')
            project_mode = self._get_project_mode(step)

            prod_steps = list()
            campaigns = dict()

            input_data_name = self.get_step_input_data_name(step)
            result = self.rucio_client.get_datasets_and_containers(input_data_name, datasets_contained_only=True)

            for name in result['datasets']:
                try:
                    name_dict = self.parse_data_name(name)
                    name_prod_step = name_dict['prod_step']
                    if not name_prod_step in prod_steps:
                        prod_steps.append(name_prod_step)
                    campaign = self.get_dataset_subcampaign(name)
                    if campaign:
                        if not campaign in campaigns.keys():
                            campaigns[campaign] = list()
                        campaigns[campaign].append(name)
                except Exception as ex:
                    raise Exception(
                        'Processing of sub-campaign/campaign or production step failed: {0}'.format(str(ex)))
            if len(prod_steps) > 1:
                task_config = self._get_task_config(step)
                task_config_changed = False
                if not 'forceSplitInput'.lower() in project_mode.keys():
                    task_config['project_mode'] = 'forceSplitInput=yes;{0}'.format(task_config.get('project_mode', ''))
                    task_config_changed = True
                if not 'useContainerName'.lower() in project_mode.keys():
                    task_config['project_mode'] = 'useContainerName=yes;{0}'.format(task_config.get('project_mode', ''))
                    task_config_changed = True
                if task_config_changed:
                    self._set_task_config(step, task_config)
                    project_mode = self._get_project_mode(step)
            if len(campaigns.keys()) > 1:
                if not 'forceSplitInput'.lower() in project_mode.keys():
                    task_config = self._get_task_config(step)
                    task_config['project_mode'] = 'forceSplitInput=yes;{0}'.format(task_config.get('project_mode', ''))
                    self._set_task_config(step, task_config)
                    project_mode = self._get_project_mode(step)
                if 'runOnlyCampaign'.lower() in project_mode.keys():
                    requested_campaigns = list()
                    for value in str(project_mode['runOnlyCampaign'.lower()]).split(','):
                        for e in TaskDefConstants.DEFAULT_SC_HASHTAGS.keys():
                            for pattern in TaskDefConstants.DEFAULT_SC_HASHTAGS[e]:
                                if re.match(r'{0}'.format(pattern), value) and (not e in requested_campaigns):
                                    requested_campaigns.append(e)
                    requested_datasets = list()
                    for requested_campaign in requested_campaigns:
                        if requested_campaign in campaigns.keys():
                            requested_datasets.extend(campaigns[requested_campaign])
                    if len(requested_datasets) > 0:
                        result['datasets'] = requested_datasets
                    else:
                        raise NoRequestedCampaignInput()
                else:
                    requested_campaign = str(step.request.subcampaign)
                    if requested_campaign.lower().startswith('MC16'.lower()) and \
                            step.request.request_type.lower() == 'MC'.lower():
                        requested_datasets = list()
                        if requested_campaign in campaigns.keys():
                            requested_datasets.extend(campaigns[requested_campaign])
                        if len(requested_datasets) > 0:
                            result['datasets'] = requested_datasets
                        else:
                            raise NoRequestedCampaignInput()

            force_merge_container = None
            if 'mergeCont'.lower() in project_mode.keys():
                option_value = str(project_mode['mergeCont'.lower()])
                if option_value.lower() == 'yes'.lower():
                    force_merge_container = True

            use_default_splitting_rule = True
            if 'forceSplitInput'.lower() in project_mode.keys():
                option_value = str(project_mode['forceSplitInput'.lower()])
                if option_value.lower() == 'yes'.lower():
                    use_default_splitting_rule = False

            reuse_input = None
            if 'reuseInput'.lower() in project_mode.keys():
                option_value = int(project_mode['reuseInput'.lower()])
                if option_value > 0:
                    reuse_input = option_value
            # TODO
            # use_input_container = False
            if use_default_splitting_rule and \
                    (prod_step.lower() == 'evgen'.lower() or prod_step.lower() == 'simul'.lower()
                     or force_merge_container):
                # use_input_container = True

                # skip_data_verify = False
                # use_real_events = False

                # dataset = self.get_step_input_data_name(step)
                # if not self.verify_container_consistency(dataset):
                #     use_input_container = False
                #     skip_data_verify = True
                #     use_real_events = True

                # if use_input_container:

                # using container for Evgen and Simul
                # if prod_step.lower() == 'evgen'.lower():
                #     # split large tasks
                #     # FIXME: define constants, review algorithm
                #     if step.input_events > 0:
                #         number_events_requested = int(step.input_events)
                #         input_data_name = self.get_step_input_data_name(step)
                #         max_number_events = 2000000
                #         number_parts = number_events_requested / max_number_events
                #         offset = 0
                #         if number_parts:
                #             splitting_dict[step.id] = list()
                #             for _ in range(number_parts):
                #                 splitting_dict[step.id].append({'dataset': input_data_name,
                #                                                 'offset': offset,
                #                                                 'number_events': max_number_events})
                #                 offset += max_number_events
                #             remain_number_events = number_events_requested % max_number_events
                #             if remain_number_events:
                #                 splitting_dict[step.id].append({'dataset': input_data_name,
                #                                                 'offset': offset,
                #                                                 'number_events': remain_number_events})
                return splitting_dict

            # task_config = self._get_task_config(step)
            # if not 'nEventsPerInputFile' in task_config.keys():
            #     logger.info("Step = %d, nEventsPerInputFile is missing, skipping the step" % step.id)
            #     return  splitting_dict

            if 'skipFilesUsedBy'.lower() in project_mode.keys():
                previous_task_id = int(project_mode['skipFilesUsedBy'.lower()])
                if previous_task_id:
                    job_params = self.task_reg.get_task_parameter(previous_task_id, 'jobParameters')
                    primary_input = self._get_primary_input(job_params)
                    if primary_input:
                        splitting_dict[step.id] = list()
                        splitting_dict[step.id].append({'dataset': primary_input['dataset'],
                                                        'offset': 0,
                                                        'number_events': int(step.input_events),
                                                        'container': None})
                        return splitting_dict

            task_config = self._get_task_config(step)
            if 'previous_task_list' in task_config.keys():
                previous_task_list = ProductionTask.objects.filter(id__in=task_config['previous_task_list'])
                for previous_task in previous_task_list:
                    job_params = self.task_reg.get_task_parameter(previous_task.id, 'jobParameters')
                    primary_input = self._get_primary_input(job_params)
                    if primary_input:
                        if not step.id in splitting_dict.keys():
                            splitting_dict[step.id] = list()
                        splitting_dict[step.id].append({'dataset': primary_input['dataset'],
                                                        'offset': 0,
                                                        'number_events': int(step.input_events),
                                                        'container': None})
                if splitting_dict:
                    return splitting_dict

            if reuse_input and len(result['datasets']) == 1:
                for i in range(reuse_input):
                    if not step.id in splitting_dict.keys():
                        splitting_dict[step.id] = list()
                    splitting_dict[step.id].append({'dataset': result['datasets'][0], 'offset': 0,
                                                    'number_events': int(step.input_events), 'container': None})
                return splitting_dict

            if not self.rucio_client.is_dsn_container(input_data_name):
                return splitting_dict

            # if not 'nEventsPerInputFile' in task_config.keys():
            #     nevents_per_files = self.get_events_per_file(input_data_name)
            #     if not nevents_per_files:
            #         logger.info("Step = %d, nEventsPerInputFile is missing, skipping the step" % step.id)
            #         return splitting_dict
            #     task_config['nEventsPerInputFile'] = nevents_per_files
            #     log_msg = "_get_splitting_dict, step = %d, input_data_name = %s, found nEventsPerInputFile = %d" % \
            #               (step.id, input_data_name, task_config['nEventsPerInputFile'])
            #     logger.info(log_msg)

            # TODO
            # if not skip_data_verify:
            #     self.verify_data_uniform(step, input_data_name)

            use_real_events = True
            if 'useRealEventsCont'.lower() in project_mode.keys():
                option_value = str(project_mode['useRealEventsCont'.lower()])
                if option_value.lower() == 'yes'.lower():
                    use_real_events = True
                else:
                    use_real_events = False

            logger.info("Step = %d, container = %s, list of datasets = %s" %
                        (step.id, input_data_name, result['datasets']))
            # events_per_file = int(task_config['nEventsPerInputFile'])
            # number_events_in_container = events_per_file * self.ddm_wrapper.ddm_get_number_files(input_data_name)
            number_events_in_container = \
                self.get_events_in_datasets(result['datasets'], step, use_real_events=use_real_events)
            if not number_events_in_container:
                raise Exception(
                    'Container {0} has no events or there is no information in AMI/Rucio'.format(input_data_name))
            # number_events_in_container = self.get_events_in_container(step, input_data_name, content=content)
            # TODO
            # number_events_in_container = self.get_events_in_container(step, input_data_name,
            #                                                           use_real_events=use_real_events)
            logger.info("Step = %d, container = %s, number_events_in_container = %d" %
                        (step.id, input_data_name, number_events_in_container))
            if not number_events_in_container:
                logger.info("Step = %d, container %s is empty or nEventsPerInputFile is missing, skipping the step" %
                            (step.id, input_data_name))
                return splitting_dict

            number_events_processed = self._get_number_events_processed(step, result['datasets'])
            logger.info("Step = %d, number_events_processed = %d" % (step.id, number_events_processed))

            if step.input_events > 0:
                number_events_requested = int(step.input_events)
            else:
                number_events_requested = number_events_in_container - number_events_processed

            if number_events_requested <= 0:
                raise NotEnoughEvents()

            if (number_events_requested + number_events_processed) > number_events_in_container:
                number_events_available = number_events_in_container - number_events_processed
                events_remains = \
                    float(number_events_requested - number_events_available) / float(number_events_requested) * 100
                if events_remains <= 10:
                    number_events_requested = number_events_available
                else:
                    raise NotEnoughEvents()
            if (step.input_events <= 0) and (step.request.request_type.lower() in ['GROUP'.lower()]):
                processed_datasets = self._get_processed_datasets(step, result['datasets'])
                for dataset_name in result['datasets']:
                    if dataset_name.split(':')[-1] not in processed_datasets:
                        events_per_file = self.get_events_per_input_file(step, dataset_name,
                                                                         use_real_events=use_real_events)
                        if not events_per_file:
                            logger.info(
                                "Step = %d, nEventsPerInputFile for dataset %s is missing, skipping this dataset" %
                                (step.id, dataset_name))
                            return splitting_dict
                        number_events = events_per_file * self.rucio_client.get_number_files(
                            dataset_name)
                        if number_events:
                            if not step.id in splitting_dict.keys():
                                splitting_dict[step.id] = list()
                            splitting_dict[step.id].append({'dataset': dataset_name, 'offset': 0,
                                                            'number_events': number_events,
                                                            'container': input_data_name})
                return splitting_dict
            start_offset = 0
            for dataset_name in result['datasets']:
                offset = 0
                number_events = 0
                events_per_file = self.get_events_per_input_file(step, dataset_name, use_real_events=use_real_events)
                # TODO
                # events_per_file = self.get_events_per_input_file(step, dataset_name, use_real_events=use_real_events)
                if not events_per_file:
                    logger.info("Step = %d, nEventsPerInputFile for dataset %s is missing, skipping this dataset" %
                                (step.id, dataset_name))
                    return splitting_dict
                number_events_in_dataset = events_per_file * self.rucio_client.get_number_files(dataset_name)
                try:
                    if (start_offset + number_events_in_dataset) < number_events_processed:
                        # skip dataset, all events are processed
                        continue
                    offset = number_events_processed - start_offset
                    if number_events_requested > number_events_in_dataset - offset:
                        number_events = number_events_in_dataset - offset
                    else:
                        number_events = number_events_requested
                        # break, all events are requested
                        break
                finally:
                    start_offset += number_events_in_dataset
                    number_events_requested -= number_events
                    number_events_processed += number_events
                    if number_events:
                        if not step.id in splitting_dict.keys():
                            splitting_dict[step.id] = list()
                        splitting_dict[step.id].append({'dataset': dataset_name, 'offset': offset / events_per_file,
                                                        'number_events': number_events, 'container': input_data_name})
        return splitting_dict

    def _get_evgen_input_list(self, step):
        evgen_input_list = list()
        input_data_name = self.get_step_input_data_name(step)
        task_config = self._get_task_config(step)
        ctag_name = step.step_template.ctag
        ctag = self._get_ami_tag_cached(ctag_name)
        energy_gev = self._get_energy(step, ctag)
        input_params = self.get_input_params(step, step, False, energy_gev, False)
        container_name_key = None
        container_name = None
        for key in input_params.keys():
            if re.match(r'^(--)?input.*File$', key, re.IGNORECASE):
                container_name_key = key
                container_name = input_params[key][0]
                break

        if not container_name:
            raise Exception('No input container found')

        if 'nFilesPerJob' in input_params.keys() and not 'nFilesPerJob' in task_config.keys():
            task_config.update({'nFilesPerJob': int(input_params['nFilesPerJob'])})

        if 'previous_task_list' in task_config.keys():
            previous_task_list = ProductionTask.objects.filter(
                id__in=task_config['previous_task_list'])
            for previous_task in previous_task_list:
                jedi_task = TTask.objects.get(id=previous_task.id)
                task_params = json.loads(jedi_task.jedi_task_param)
                job_params = task_params['jobParameters']
                random_seed = self._get_job_parameter('randomSeed', job_params)
                dsn = self._get_primary_input(job_params)['dataset'].split(':')[-1]
                offset = 0
                if random_seed:
                    offset = int(random_seed['offset'])
                else:
                    raise Exception('There is no randomSeed parameter in the previous task')
                nfiles = int(task_params.get('nFiles'))
                nfiles_per_job = int(task_params.get('nFilesPerJob'))
                nevents_per_job = int(task_params.get('nEventsPerJob'))
                if not nfiles or not nfiles_per_job or not nevents_per_job:
                    raise Exception(
                        'Necessary task parameters are missing in the previous task')
                round_up = lambda num: int(num + 1) if int(num) != num else int(num)
                input_params_split = copy.deepcopy(input_params)
                input_params_split['nevents'] = round_up(float(nfiles * nevents_per_job) / float(nfiles_per_job))
                input_params_split['nfiles'] = nfiles
                input_params_split['offset'] = offset
                input_params_split['event_offset'] = offset * nevents_per_job / nfiles_per_job
                input_params_split[container_name_key] = list([dsn])
                evgen_input_list.append(input_params_split)
            return evgen_input_list

        datasets = self.rucio_client.list_datasets_in_container(container_name)

        nfiles_used = 0
        task = None
        task_list = \
            ProductionTask.objects.filter(
                ~Q(status__in=['failed', 'broken', 'aborted', 'obsolete', 'toabort']) &
                (Q(step__slice__input_data=input_data_name) |
                 Q(step__slice__input_data__endswith=input_data_name.split(':')[-1])),
                project=step.request.project,
                step__step_template__ctag=step.step_template.ctag).order_by(
                '-id')
        for previous_task in task_list:
            requested_output_types = step.step_template.output_formats.split('.')
            previous_output_types = previous_task.step.step_template.output_formats.split('.')
            processed_output_types = [e for e in requested_output_types if e in previous_output_types]
            if not processed_output_types:
                continue
            task = previous_task
            break

        task_dsn_no_scope = None
        if task:
            jedi_task = TTask.objects.get(id=task.id)
            task_params = json.loads(jedi_task.jedi_task_param)
            task_random_seed = \
                self._get_job_parameter('randomSeed', task_params['jobParameters'])
            task_dsn_no_scope = \
                self._get_primary_input(task_params['jobParameters'])['dataset'].split(':')[-1]
            offset = 0
            if task_random_seed:
                offset = int(task_random_seed['offset'])
            nfiles_used = offset
            if 'nFiles' in task_params:
                nfiles_used += int(task_params['nFiles'])

        nevents_per_job = input_params.get('nEventsPerJob')
        if not nevents_per_job:
            raise Exception(
                'JO file {0} does not contain evgenConfig.minevents definition. '.format(
                    input_data_name) +
                'The task is rejected')
        nfiles_per_job = 1
        if 'nFilesPerJob' in task_config.keys():
            nfiles_per_job = int(task_config['nFilesPerJob'])

        nfiles_requested = int(step.input_events) * nfiles_per_job / nevents_per_job
        nfiles = 0
        files_used_count = nfiles_used
        files_requested_count = nfiles_requested

        for dsn in datasets:
            dsn_no_scope = dsn.split(':')[-1]
            nfiles_in_ds = self.rucio_client.get_number_files_from_metadata(dsn)
            files_used_count -= nfiles_in_ds
            if files_used_count >= 0:
                continue
            if nfiles_in_ds + files_used_count > 0:
                continue
            if dsn_no_scope == task_dsn_no_scope:
                continue
            input_params_split = copy.deepcopy(input_params)
            files_requested_count -= nfiles_in_ds
            if files_requested_count > 0:
                input_params_split['nevents'] = nfiles_in_ds * nevents_per_job / nfiles_per_job
                input_params_split['nfiles'] = nfiles_in_ds
                input_params_split['offset'] = nfiles_used + nfiles
                nfiles += nfiles_in_ds
                input_params_split['event_offset'] = \
                    input_params_split['offset'] * nevents_per_job / nfiles_per_job
                input_params_split[container_name_key] = list([dsn])
                evgen_input_list.append(input_params_split)
            else:
                input_params_split['nevents'] = \
                    (nfiles_requested - nfiles) * nevents_per_job / nfiles_per_job
                input_params_split['nfiles'] = (nfiles_requested - nfiles)
                input_params_split['offset'] = nfiles_used + nfiles
                nfiles += (nfiles_requested - nfiles)
                input_params_split['event_offset'] = \
                    input_params_split['offset'] * nevents_per_job / nfiles_per_job
                input_params_split[container_name_key] = list([dsn])
                evgen_input_list.append(input_params_split)
                break
        if nfiles < nfiles_requested:
            raise Exception(
                'No more input files in {0}. Only {1} files are available'.format(container_name, nfiles)
            )
        if not evgen_input_list:
            raise Exception(
                'No unprocessed datasets in the container {0}'.format(container_name)
            )
        return evgen_input_list

    def _build_linked_step_list(self, req, input_slice):
        # Approved
        step_list = list(StepExecution.objects.filter(request=req,
                                                      status=self.protocol.STEP_STATUS[StepStatus.APPROVED],
                                                      slice=input_slice))
        result_list = []
        temporary_list = []
        another_chain_step = None
        for step in step_list:
            if step.step_parent_id == step.id:
                if result_list:
                    raise ValueError('Not linked chain')
                else:
                    result_list.append(step)
            else:
                temporary_list.append(step)
        if not result_list:
            for index, current_step in enumerate(temporary_list):
                step_parent = StepExecution.objects.get(id=current_step.step_parent_id)
                if step_parent not in temporary_list:
                    # step in other chain
                    another_chain_step = step_parent
                    result_list.append(current_step)
                    temporary_list.pop(index)
        for i in range(len(temporary_list)):
            j = 0
            while temporary_list[j].step_parent_id != result_list[-1].id:
                j += 1
                if j >= len(temporary_list):
                    raise ValueError('Not linked chain')
            result_list.append(temporary_list[j])
        return result_list, another_chain_step

    def _get_request_status(self, request):
        statuses = TRequestStatus.objects.filter(request=request).order_by('-timestamp').values_list(
            'status', flat=True)
        for status in statuses:
            if status not in ['approved', 'comment']:
                if status == 'waiting':
                    return 'working'
                else:
                    return status

    def _define_tasks_for_requests(self, requests, jira_client, restart=False):
        for request in requests:
            request.locked = True
            request.save()
            logger.info("Request %d is locked" % request.id)
        logger.info("Processing production requests")
        logger.info("Requests to process: %s" % str([int(req.id) for req in requests]))

        for request in requests:
            try:
                logger.info("Processing request %d" % request.id)
                exception = False
                first_steps = list()
                for input_slice in InputRequestList.objects.filter(request=request).order_by('slice'):
                    steps_in_slice = StepExecution.objects.filter(request=request,
                                                                  status=self.protocol.STEP_STATUS[StepStatus.APPROVED],
                                                                  slice=input_slice).order_by('id')
                    # FIXME
                    try:
                        steps_in_slice, _ = self._build_linked_step_list(request, input_slice)
                    except Exception as ex:
                        logger.exception("_build_linked_step_list failed: %s" % str(ex))

                    if steps_in_slice:
                        for step in steps_in_slice:
                            if not self.task_reg.get_step_output(step.id, exclude_failed=False) or restart:
                                first_steps.append(step)
                                break
                logger.info("Request = %d, chains: %s" % (request.id, str([int(st.id) for st in first_steps])))
                TaskRegistration.register_request_reference(request)
                for step in first_steps:
                    step_parent = StepExecution.objects.get(id=step.step_parent_id)
                    if step_parent.status == self.protocol.STEP_STATUS[StepStatus.WAITING]:
                        continue
                    try:
                        phys_cont_list = list()
                        evgen_input_list = list()
                        input_data_name = self.get_step_input_data_name(step)
                        if input_data_name:
                            input_data_dict = self.parse_data_name(input_data_name)

                            project_mode = self._get_project_mode(step)
                            force_split_evgen = None
                            if 'splitEvgen'.lower() in project_mode.keys():
                                option_value = str(project_mode['splitEvgen'.lower()])
                                if option_value.lower() == 'yes'.lower():
                                    force_split_evgen = True

                            if str(input_data_dict['number']).lower().startswith('period'.lower()) \
                                    or input_data_dict['prod_step'].lower() == 'PhysCont'.lower():
                                input_params = self.get_input_params(step, step, None, 0, False)
                                if not input_params:
                                    raise Exception("No datasets in the period container %s" % input_data_name)
                                for key in input_params.keys():
                                    if re.match(r'^(--)?input.*File$', key, re.IGNORECASE):
                                        phys_cont_list.extend(input_params[key])
                            elif input_data_dict['prod_step'].lower() == 'py'.lower() and force_split_evgen:
                                evgen_input_list.extend(self._get_evgen_input_list(step))
                        if phys_cont_list:
                            for input_dataset in phys_cont_list:
                                try:
                                    self.create_task_chain(step.id, input_dataset=input_dataset)
                                except (TaskDuplicateDetected, NoMoreInputFiles, ParentTaskInvalid,
                                        UnmergedInputProcessedException) as ex:
                                    log_msg = \
                                        'Request = {0}, Chain = {1} ({2}), input = {3}, exception occurred: {4}'.format(
                                            request.id, step.slice.slice, step.id, self.get_step_input_data_name(step),
                                            get_exception_string())
                                    jira_client.log_exception(request.reference, ex, log_msg=log_msg)
                                    exception = True
                                    continue
                                except Exception as ex:
                                    raise ex
                        elif evgen_input_list:
                            for input_params in evgen_input_list:
                                try:
                                    self.create_task_chain(step.id,
                                                           first_step_number_of_events=input_params['nevents'],
                                                           evgen_params=input_params)
                                except (TaskDuplicateDetected, NoMoreInputFiles, ParentTaskInvalid,
                                        UnmergedInputProcessedException) as ex:
                                    log_msg = \
                                        'Request = {0}, Chain = {1} ({2}), input = {3}, exception occurred: {4}'.format(
                                            request.id, step.slice.slice, step.id, self.get_step_input_data_name(step),
                                            get_exception_string())
                                    jira_client.log_exception(request.reference, ex, log_msg=log_msg)
                                    exception = True
                                    continue
                                except Exception as ex:
                                    raise ex
                        else:
                            use_parent_output = None
                            if step.id != step.step_parent_id:
                                parent_step = StepExecution.objects.get(id=step.step_parent_id)
                                if parent_step.status.lower() == self.protocol.STEP_STATUS[StepStatus.APPROVED].lower():
                                    use_parent_output = True
                                elif parent_step.status.lower() == \
                                        self.protocol.STEP_STATUS[StepStatus.NOTCHECKED].lower():
                                    raise Exception("Parent step is '{0}'".format(parent_step.status))
                            # self.create_task_chain(step.id, restart=use_parent_output)
                            splitting_dict = dict()
                            try:
                                if not use_parent_output:
                                    splitting_dict = self._get_splitting_dict(step)
                            except NotEnoughEvents:
                                raise Exception('Not enough events')
                            except UniformDataException as ex:
                                raise ex
                            except NoRequestedCampaignInput:
                                raise Exception('No input for specified campaign')
                            except:
                                raise
                            if not step.id in splitting_dict.keys():
                                if use_parent_output:
                                    parent_step = StepExecution.objects.get(id=step.step_parent_id)
                                    for task_id in self.task_reg.get_step_tasks(parent_step.id):
                                        try:
                                            self.create_task_chain(step.id, restart=use_parent_output,
                                                                   first_step_number_of_events=-1,
                                                                   first_parent_task_id=task_id)
                                        except (TaskDuplicateDetected, NoMoreInputFiles, ParentTaskInvalid,
                                                UnmergedInputProcessedException) as ex:
                                            log_msg = \
                                                'Request = {0}, Chain = {1} ({2}), input = {3}, exception occurred: {4}'.format(
                                                    request.id, step.slice.slice, step.id,
                                                    self.get_step_input_data_name(step),
                                                    get_exception_string())
                                            jira_client.log_exception(request.reference, ex, log_msg=log_msg)
                                            exception = True
                                            continue
                                        except Exception as ex:
                                            raise ex
                                else:
                                    self.create_task_chain(step.id, restart=use_parent_output)
                            else:
                                for step_input in splitting_dict[step.id]:
                                    try:
                                        self.create_task_chain(step.id, restart=use_parent_output,
                                                               input_dataset=step_input['dataset'],
                                                               first_step_number_of_events=step_input['number_events'],
                                                               primary_input_offset=step_input['offset'],
                                                               container_name=step_input['container'])
                                    except (TaskDuplicateDetected, NoMoreInputFiles, ParentTaskInvalid,
                                            UnmergedInputProcessedException, UniformDataException) as ex:
                                        log_msg = \
                                            'Request = {0}, Chain = {1} ({2}), input = {3}, exception occurred: {4}'.format(
                                                request.id, step.slice.slice, step.id,
                                                self.get_step_input_data_name(step), get_exception_string())
                                        jira_client.log_exception(request.reference, ex, log_msg=log_msg)
                                        exception = True
                                        continue
                                    except Exception as ex:
                                        raise ex
                    except KeyboardInterrupt:
                        pass
                    except:
                        log_msg = "Request = %d, Chain = %d (%d), input = %s, exception occurred: %s" % \
                                  (request.id, step.slice.slice, step.id, self.get_step_input_data_name(step),
                                   get_exception_string())  # str(ex))
                        logger.exception(log_msg)
                        if request.reference:
                            try:
                                jira_client.add_issue_comment(request.reference, log_msg)
                                exception = True
                            except:
                                pass
                        continue
                request.status = self._get_request_status(request)
                request.exception = exception
                request.save()
                logger.info("Request = %d, status = %s" % (request.id, request.status))
            finally:
                # unlock request
                request.locked = False
                request.save()
                logger.info("Request %s is unlocked" % request.id)

    def force_process_requests(self, requests_ids, restart=False):
        jira_client = JIRAClient()
        try:
            jira_client.authorize()
        except Exception as ex:
            logger.exception('JIRAClient::authorize failed: {0}'.format(str(ex)))
        requests = TRequest.objects.filter(id__in=requests_ids)
        self._define_tasks_for_requests(requests, jira_client, restart)

    def process_requests(self, restart=False, no_wait=False, debug_only=False, request_types=None):
        jira_client = JIRAClient()
        try:
            jira_client.authorize()
        except Exception as ex:
            logger.exception('JIRAClient::authorize failed: {0}'.format(str(ex)))
        request_status = self.protocol.REQUEST_STATUS[RequestStatus.APPROVED]
        if not debug_only:
            requests = TRequest.objects.filter(~Q(locked=True) & ~Q(description__contains='_debug'),
                                               id__gt=800,
                                               status=request_status).order_by('id')
        else:
            requests = TRequest.objects.filter(~Q(locked=True),
                                               description__contains='_debug',
                                               id__gt=800,
                                               status=request_status).order_by('id')
        if request_types and requests:
            requests = requests.filter(request_type__in=request_types)
        if not requests:
            return
        ready_request_list = list()
        for request in requests:
            is_fast = request.is_fast or False
            last_access_timestamp = \
                TRequestStatus.objects.filter(request=request, status=request_status).order_by('-id')[0].timestamp
            now = timezone.now()
            time_offset = (now - last_access_timestamp).seconds
            if (time_offset // 3600) < REQUEST_GRACE_PERIOD:
                if (not no_wait) and (not is_fast):
                    logger.info("Request %d is skipped, approved at %s" % (request.id, last_access_timestamp))
                    continue
            ready_request_list.append(request)
        requests = ready_request_list[:1]
        self._define_tasks_for_requests(requests, jira_client, restart)
