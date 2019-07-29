__author__ = 'Dmitry Golubkov'

import os
import json
from pydoc import locate
from taskengine.models import InstalledSW
from taskengine.agisclient import AGISClient
from taskengine.protocol import TaskDefConstants


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
        task_config = self.get_task_config(step)
        if 'project_mode' in task_config.keys():
            project_mode.update(self._parse_project_mode(task_config['project_mode']))

        project_mode_options = self.get_options()

        option_names = {key.lower(): key for key in project_mode_options.keys()}

        for key in project_mode.keys():
            if not key in option_names.keys():
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
            for key in content.keys():
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
            if not '=' in option:
                raise Exception('The project_mode option \"{0}\" has invalid format. '.format(option) +
                                'Expected format is \"optionName=optionValue\"')
            project_mode_dict.update({option.split('=')[0].lower(): option.split('=')[1]})
        return project_mode_dict

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

    def set_cmtconfig(self):
        if self.cmtconfig and self.cache and not self.skipCMTConfigCheck:
            if not self._is_cmtconfig_exist(self.cache, self.cmtconfig):
                available_cmtconfig_list = self._get_cmtconfig_list(self.cache)
                raise Exception(
                    'cmtconfig \"{0}\" specified by user is not exist in cache \"{1}\" (available: \"{2}\")'.format(
                        self.cmtconfig, self.cache, str(', '.join(available_cmtconfig_list))))

        if not self.cmtconfig and self.use_nightly_release:
            raise Exception('cmtconfig parameter must be specified in project_mode when nightly release is used')

        if not self.cmtconfig:
            setattr(self, 'cmtconfig', TaskDefConstants.DEFAULT_PROJECT_MODE['cmtconfig'])
            if self.cache:
                cmtconfig_list = self._get_cmtconfig_list(self.cache)
                if len(cmtconfig_list) == 1:
                    setattr(self, 'cmtconfig', cmtconfig_list[0])
                else:
                    if len(cmtconfig_list) > 1:
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
                    if not self.cmtconfig in cmtconfig_list:
                        if len(cmtconfig_list) > 0:
                            setattr(self, 'cmtconfig', cmtconfig_list[0])
                        else:
                            raise Exception(
                                'Default cmtconfig \"{0}\" is not exist in cache \"{1}\" (available: \"{2}\")'.format(
                                    self.cmtconfig, self.cache, str(','.join(cmtconfig_list))))
