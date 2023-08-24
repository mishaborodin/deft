from taskengine.metadata import AMIClient

__author__ = 'Dmitry Golubkov'

import os
import json
from pydoc import locate
from taskengine.agisclient import AGISClient
from taskengine.protocol import TaskDefConstants
from deftcore.log import Logger

logger = Logger.get()
class UnknownProjectModeOption(Exception):
    def __init__(self, option_key):
        super(UnknownProjectModeOption, self).__init__('Invalid project_mode option: {0}'.format(option_key))


class InvalidProjectModeOptionValue(Exception):
    def __init__(self, key, value):
        super(InvalidProjectModeOptionValue, self).__init__(
            'Invalid project_mode option value: {0}=\"{1}\"'.format(key, value))


class ProjectMode(object):
    def __init__(self, step, cache=None, use_nightly_release=False):
        """
        :param step: object of StepExecution
        :param cache: string in format 'CacheName-CacheRelease', for example, 'AtlasProduction-19.2.1.2'
        :return: project_mode dict
        """
        self.project_mode_dict = dict()
        self.cache = cache
        self.use_nightly_release = use_nightly_release
        self.agis_client = AGISClient()

        project_mode = dict()
        self.task_config = self.get_task_config(step)
        if 'project_mode' in list(self.task_config.keys()):
            project_mode.update(self._parse_project_mode(self.task_config['project_mode']))

        project_mode_options = self.get_options()

        option_names = {key.lower(): key for key in list(project_mode_options.keys())}

        for key in list(project_mode.keys()):
            if key not in list(option_names.keys()):
                raise UnknownProjectModeOption(key)
            option_type = locate(project_mode_options[option_names[key]]['type'])
            option_value = project_mode[key]
            if option_type == bool:
                if option_value == 'yes':
                    option_value = True
                elif option_value == 'no':
                    option_value = False
                else:
                    raise InvalidProjectModeOptionValue(option_names[key], option_value)
            setattr(self, option_names[key], option_type(option_value))
            self.project_mode_dict.update({option_names[key]: option_type(option_value)})

        self.set_cmtconfig()
        self.set_cmtconfig_options()
        self.project_mode_dict['cmtconfig'] = self.cmtconfig

    def __getattr__(self, item):
        return None

    @staticmethod
    def get_options():
        path = '{0}{1}projectmode.json'.format(os.path.dirname(__file__), os.path.sep)
        with open(path, 'r') as fp:
            return json.loads(fp.read())

    @staticmethod
    def get_task_config(step):
        task_config = dict()
        if step.task_config:
            content = json.loads(step.task_config)
            for key in list(content.keys()):
                if content[key] is None or content[key] == '':
                    continue
                task_config.update({key: content[key]})
        return task_config

    @staticmethod
    def set_task_config(step, task_config, keys_to_save=None):
        if keys_to_save is None:
            step.task_config = json.dumps(task_config)
            step.save(update_fields=['task_config'])
        else:
            if len(keys_to_save) > 0:
                config = {key: task_config[key] for key in keys_to_save}
                if config:
                    step_task_config = ProjectMode.get_task_config(step)
                    step_task_config.update(config)
                    step.task_config = json.dumps(step_task_config)
                    step.save(update_fields=['task_config'])

    @staticmethod
    def _parse_project_mode(project_mode_string):
        project_mode_dict = dict()
        for option in project_mode_string.replace(' ', '').split(';'):
            if not option:
                continue
            if '=' not in option:
                raise Exception('The project_mode option \"{0}\" has invalid format. '.format(option) +
                                'Expected format is \"optionName=optionValue\"')
            project_mode_dict.update({option.split('=')[0].lower(): option[option.find('=')+1:]})
        return project_mode_dict

    def _is_cmtconfig_exist(self, cache, cmtconfig):
        agis_cmtconfig_list = self.agis_client.get_cmtconfig(cache)
        if not agis_cmtconfig_list:
            try:
                agis_cmtconfig_list = self._get_cmtconfig_from_cvmfs(cache)
            except:
                agis_cmtconfig_list = []
        return cmtconfig in agis_cmtconfig_list

    def _get_cmtconfig_list(self, cache):
        agis_cmtconfig_list = self.agis_client.get_cmtconfig(cache)
        return list(agis_cmtconfig_list)

    def _get_cmtconfig_from_cvmfs(self, cache):
        release = cache.split('-')[-1]
        project = cache.split('-')[0]
        path = TaskDefConstants.DEAFULT_SW_RELEASE_PATH.format(release=release,project=project,release_base=".".join(release.split(".")[:2]))
        cmt_config_from_cvmfs = [name for name in os.listdir(path) if os.path.isdir(os.path.join(path, name))]
        return cmt_config_from_cvmfs


    def set_cmtconfig_options(self):
        if self.cache:
            if self.cmtconfig and '#' not in self.cmtconfig:
               if self.cmtconfig.startswith('aarch64'):
                   self.cmtconfig = f'{self.cmtconfig}#aarch64'
               else:
                    release = self.cache.split('-')[-1]
                    version_parts = release.split('.')
                    version = int(version_parts[0]) * 10000 + int(version_parts[1]) * 100 + int(version_parts[2])
                    if version >= 240010:
                        self.cmtconfig = f'{self.cmtconfig}#x86_64-*-v2'


    def set_cmtconfig(self):

        if self.use_nightly_release and (not self.cmtconfig or not self.skipCMTConfigCheck):
            raise Exception('cmtconfig parameter and skipCMTConfigCheck must be specified in project_mode when nightly release is used')

        if not self.container_name and 'container_name' in self.task_config:
            self.container_name = self.task_config['container_name']
        if self.cmtconfig and self.cache and not self.skipCMTConfigCheck:
            architecture = self.cmtconfig.split('#')[0]
            if self.container_name:
                ami_client = AMIClient()
                if ami_client.ami_container_exists(self.container_name):
                    ami_cmtconfig = ami_client.ami_cmtconfig_by_image_name(self.container_name)
                    if architecture != ami_cmtconfig:
                        raise Exception(
                            'cmtconfig \"{0}\" specified by the user does not correspond one in the container \"{1}\" '.format(
                                self.cmtconfig, ami_cmtconfig))
            else:
                if not self._is_cmtconfig_exist(self.cache, architecture):
                    available_cmtconfig_list = self._get_cmtconfig_list(self.cache)
                    raise Exception(
                        'cmtconfig \"{0}\" specified by user is not exist in cache \"{1}\" (available: \"{2}\")'.format(
                            self.cmtconfig, self.cache, str(', '.join(available_cmtconfig_list))))
        if self.container_name and self.cache:
            cache_exists = False
            ami_client = AMIClient()
            sw_tags_per_cache = ami_client.ami_sw_tag_by_cache(self.cache.replace('-','_'))
            for sw_tag in sw_tags_per_cache:
                if sw_tag['TAGNAME'] in self.container_name:
                    cache_exists = True
                    break
            if not cache_exists and not self.skipCMTConfigCheck:
                raise Exception(
                    'Cache \"{0}\" is not found in the container \"{1}\" '.format(
                        self.cache, self.container_name))



        if not self.cmtconfig:
            setattr(self, 'cmtconfig', TaskDefConstants.DEFAULT_PROJECT_MODE['cmtconfig'])
            if self.cache:
                if self.container_name:
                        ami_client = AMIClient()
                        if ami_client.ami_container_exists(self.container_name):
                            setattr(self, 'cmtconfig', ami_client.ami_cmtconfig_by_image_name(self.container_name))
                        else:
                            raise Exception(
                                'cmtconfig is required for containers which are not registered in AMI')
                else:
                    cmtconfig_list = self._get_cmtconfig_list(self.cache)
                    if len(cmtconfig_list) == 1:
                        setattr(self, 'cmtconfig', cmtconfig_list[0])
                    else:
                        if len(cmtconfig_list) > 1:
                            cmtconfig_list.sort()
                            if (len(cmtconfig_list) == 2 and 'AthGeneration' in self.cache) and\
                                     (cmtconfig_list[0].split('-')[-2:] == cmtconfig_list[1].split('-')[-2:]) and \
                                    ('centos7' in cmtconfig_list[0])  and ('slc6' in cmtconfig_list[1]):
                                setattr(self, 'cmtconfig', cmtconfig_list[1])
                                return
                            else:
                                value = str(','.join(cmtconfig_list))
                                raise Exception(
                                    'cmtconfig is not specified but more than one cmtconfig is available ({0}).'.format(
                                        value) + ' The task is rejected')
                        # prodsys1
                        # ver_parts = step.step_template.swrelease.split('.')
                        release = self.cache.split('-')[-1]
                        ver_parts = release.split('.')
                        ver = int(ver_parts[0]) * 1000 + int(ver_parts[1]) * 100 + int(ver_parts[2])
                        if int(ver_parts[0]) <= 13:
                            setattr(self, 'cmtconfig', 'i686-slc3-gcc323-opt')
                        elif ver < 15603:
                            setattr(self, 'cmtconfig', 'i686-slc4-gcc34-opt')
                        elif ver < 19003:
                            setattr(self, 'cmtconfig', 'i686-slc5-gcc43-opt')
                        elif ver < 20100:
                            setattr(self, 'cmtconfig', 'x86_64-slc6-gcc47-opt')
                        else:
                            setattr(self, 'cmtconfig', 'x86_64-slc6-gcc48-opt')
                        if self.cmtconfig not in cmtconfig_list:
                            if len(cmtconfig_list) > 0:
                                setattr(self, 'cmtconfig', cmtconfig_list[0])
                            else:
                                logger.error(f'{self.cache} is not registered in CRIC')
                                cmtconfig_list = self._get_cmtconfig_from_cvmfs(self.cache)
                                if len(cmtconfig_list) != 1 :
                                        raise Exception(f'{self.cache} is not registered in CRIC and {cmtconfig_list} found in CVMFS')
                                else:
                                    setattr(self, 'cmtconfig', cmtconfig_list[0])
