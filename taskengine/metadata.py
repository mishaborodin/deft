__author__ = 'Dmitry Golubkov'

import re
import json
import httplib
import ast
import os
import sys
import time
from deftcore.log import Logger
from deftcore.settings import AMI_ENDPOINTS
import pyAMI.client
import pyAMI_atlas
import pyAMI.exception
import pyAMI_atlas.api
from django.core.exceptions import ObjectDoesNotExist
from taskengine.models import TTrfConfig, TProject, TDataFormat, PhysicsContainer, \
    ProductionTask, ProductionTag, StepExecution, StepTemplate
from string import Template
from deftcore.security.voms import VOMSClient

logger = Logger.get()


class AMIClient(object):
    def __init__(self):
        try:
            self.client = pyAMI.client.Client(
                AMI_ENDPOINTS,
                # key_file=self._get_proxy(),
                # cert_file=self._get_proxy(),
                ignore_proxy=True
            )
            logger.info('AMIClient, currentUser={0}'.format(self._get_current_user()))
        except Exception as ex:
            logger.critical('AMI initialization failed: {0}'.format(str(ex)))

    def _get_current_user(self):
        command = ['GetSessionInfo']
        result = self.client.execute(command, format='dom_object').get_rows('user')
        if len(result) > 0:
            return result[0].get('AMIUser', None)
        else:
            return None

    def _ami_get_tag(self, tag_name):
        command = [
            'AMIGetAMITagInfo',
            '-newStructure',
            '-amiTag="%s"' % tag_name,
        ]

        return self.client.execute(command, format='dom_object').get_rows('amiTagInfo')

    def _ami_get_tag_old(self, tag_name):
        command = [
            'AMIGetAMITagInfo',
            '-oldStructure',
            '-amiTag="%s"' % tag_name,
        ]

        return self.client.execute(command, format='dom_object').get_rows('amiTagInfo')

    def _ami_get_tag_new(self, tag_name):
        command = [
            'AMIGetAMITagInfo',
            '-hierarchicalView',
            '-amiTag="%s"' % tag_name,
        ]

        return self.client.execute(command, format='dom_object').get_rows('amiTagInfo')

    def _ami_get_tag_flat(self, tag_name):
        command = [
            'AMIGetAMITagInfoNew',
            '-amiTag="%s"' % tag_name,
        ]

        result = self.client.execute(command, format='dom_object').get_rows('amiTagInfo')
        ami_tag = result[0]
        ami_tag['transformationName'] = ami_tag['transformName']
        return [ami_tag, ]

    def _ami_list_phys_container(self, created_after=None):
        fields = [
            'logicalDatasetName',
            'created',
            'lastModified',
            'createdBy',
            'projectName',
            'dataType',
            'runNumber',
            'streamName',
            'prodStep'
        ]

        if created_after:
            conditions = \
                'WHERE dataset.amiStatus=\'VALID\' AND dataset.created >= \'%s\' ' % created_after.strftime('%Y-%m-%d')
        else:
            conditions = 'WHERE dataset.amiStatus=\'VALID\' '

        query = \
            '"SELECT %s ' % ','.join(['dataset.%s' % e for e in fields]) + \
            conditions + \
            'ORDER BY dataset.created ASC"'

        command = [
            'SearchQuery',
            '-entity="dataset"',
            '-processingStep="real_data"',
            '-project="dataSuper_001"',
            '-glite=%s' % query
        ]

        return self.client.execute(command, format='dom_object').get_rows()

    def get_nevents_per_file(self, dataset):
        dataset = dataset.split(':')[-1].strip('/')
        tid_pattern = r'(?P<tid>_tid\d+_\d{2})'
        if re.match(r"^.*%s$" % tid_pattern, dataset):
            dataset = re.sub(tid_pattern, '', dataset)
        pyAMI_atlas.api.init()
        result = pyAMI_atlas.api.get_dataset_info(self.client, dataset)
        round_up = lambda num: int(num + 1) if int(num) != num else int(num)
        return round_up(float(result[0]['totalEvents']) / float(result[0]['nFiles']))

    def get_types(self):
        return [e.name for e in TDataFormat.objects.all()]

    def ami_get_params(self, cache, release, trf_name):
        command = [
            'GetParamsForTransform',
            '-releaseName="{0}_{1}"'.format(cache, release),
            '-transformName={0}'.format(trf_name)
        ]

        result = self.client.execute(command, format='dom_object').get_rows('params')

        trf_params = list()
        for param in result:
            name = param['paramName']
            if not name.startswith('--'):
                name = "--%s" % name
            trf_params.append(name)

        return trf_params

    def is_new_ami_tag(self, ami_tag):
        if 'notAKTR' in ami_tag.keys() and ami_tag['notAKTR']:
            return True
        else:
            return False

    def apply_phconfig_ami_tag(self, ami_tag):
        if 'phconfig' in ami_tag:
            phconfig_dict = eval(ami_tag['phconfig'])
            for config_key in phconfig_dict.keys():
                if isinstance(phconfig_dict[config_key], dict):
                    value_list = list()
                    for key in phconfig_dict[config_key].keys():
                        if isinstance(phconfig_dict[config_key][key], list):
                            for value in ['{0}:{1}'.format(key, ss) for ss in phconfig_dict[config_key][key]]:
                                value_list.append(value)
                        else:
                            value = phconfig_dict[config_key][key]
                            value_list.append("%s:%s" % (key, value))
                    config_value = ' '.join([json.dumps(e) for e in value_list])
                elif isinstance(phconfig_dict[config_key], list):
                    config_value = ' '.join([json.dumps(e) for e in phconfig_dict[config_key]])
                else:
                    config_value = json.dumps(phconfig_dict[config_key])
                logger.debug("apply phconfig key=value: %s=%s" % (config_key, config_value))
                for key in ami_tag.keys():
                    if key.lower() == config_key.lower():
                        ami_tag[key] = config_value
                ami_tag.update({config_key: config_value})
                if config_key.lower() == 'geometryversion':
                    ami_tag['Geometry'] = 'none'

    def get_ami_tag_owner(self, tag_name):
        result = self._ami_get_tag(tag_name)
        ami_tag = result[0]
        return [ami_tag['createdBy'], ami_tag['created']]

    def get_ami_tag_tzero(self, tag_name):
        tzero_tag = dict()
        result = self._ami_get_tag_new(tag_name)
        tzero_tag = result[0]['dict']
        return tzero_tag

    def get_ami_tag(self, tag_name):
        ami_tag = dict()

        try:
            result = self._ami_get_tag_old(tag_name)
            ami_tag = result[0]
        except pyAMI.exception.Error as ex:
            if 'Invalid amiTag found'.lower() in ex.message.lower():
                try:
                    if tag_name.startswith('z500'):
                        result = self._ami_get_tag_flat(tag_name)
                    else:
                        result = self._ami_get_tag(tag_name)
                    ami_tag = result[0]
                    if str(ami_tag['transformationName']).endswith('.py'):
                        ami_tag['transformation'] = "%s" % ami_tag['transformationName']
                    else:
                        ami_tag['transformation'] = "%s.py" % ami_tag['transformationName']
                    ami_tag['SWReleaseCache'] = "%s_%s" % (ami_tag['groupName'], ami_tag['cacheName'])
                except Exception as ex:
                    logger.exception("[1] Exception: %s" % str(ex))
            elif '[Errno 111] Connection refused'.lower() in ex.message.lower():
                raise Exception(ex.message)
            else:
                logger.exception('pyAMI.exception.Error: {0}'.format(ex.message))
        except httplib.BadStatusLine as ex:
            raise Exception('pyAMI.exception: {0}'.format(type(ex).__name__))
        except Exception as ex:
            logger.exception("[2] Exception: %s" % str(ex))

        try:
            prodsys_tag = TTrfConfig.objects.get(tag=tag_name[0], cid=int(tag_name[1:]))

            if not ami_tag:
                ami_tag['transformation'] = prodsys_tag.trf
                ami_tag['SWReleaseCache'] = "%s_%s" % (prodsys_tag.cache, prodsys_tag.trf_version)
                ami_tag.update(dict(zip(prodsys_tag.lparams.split(','), prodsys_tag.vparams.split(','))))

            ami_tag['productionStep'] = prodsys_tag.prod_step
            ami_tag['notAKTR'] = False
        except ObjectDoesNotExist as ex:
            logger.info("The tag %s is not found in AKTR" % tag_name)
            if ami_tag:
                ami_tag['notAKTR'] = True
        except Exception as ex:
            logger.exception("Exception: %s" % str(ex))

        if not ami_tag:
            raise Exception("The configuration tag \"%s\" is not registered" % tag_name)

        return ami_tag

    def _read_trf_params(self, fp):
        trf_params = list()
        for source_line in fp.read().splitlines():
            source_line = source_line.replace(' ', '')
            if 'ListOfDefaultPositionalKeys='.lower() in source_line.lower():
                trf_params.extend(ast.literal_eval(source_line.split('=')[-1]))
                break
        return trf_params

    def _trf_dump_args(self, list_known_path, trf_transform_path):
        list_known_python_path = list()
        for path in list_known_path:
            old_str_pattern = re.compile(re.escape('share/bin'), re.IGNORECASE)
            known_python_path = old_str_pattern.sub('python', os.path.dirname(path))
            if known_python_path and os.path.exists(known_python_path):
                list_known_python_path.append(known_python_path)
        for path in list_known_python_path:
            sys.path.append(path)

        trf_transform = os.path.basename(trf_transform_path)
        sys.path.append(os.path.dirname(trf_transform_path))
        trf_module = __import__(os.path.splitext(trf_transform)[0])
        if not hasattr(trf_module, 'getTransform'):
            raise Exception("The module %s does not support for dumpArgs" % trf_transform)
        get_transform_method = getattr(trf_module, 'getTransform')
        trf = get_transform_method()
        list_key = ['--' + str(key) for key in trf.parser.allArgs if
                    key not in ('h', 'verbose', 'loglevel', 'dumpargs', 'argdict')]
        list_key.sort()
        return list_key

    def _trf_retrieve_sub_steps(self, list_known_path, trf_transform_path):
        list_known_python_path = list()
        for path in list_known_path:
            old_str_pattern = re.compile(re.escape('share/bin'), re.IGNORECASE)
            known_python_path = old_str_pattern.sub('python', os.path.dirname(path))
            if known_python_path and os.path.exists(known_python_path):
                list_known_python_path.append(known_python_path)
        for path in list_known_python_path:
            sys.path.append(path)

        trf_transform = os.path.basename(trf_transform_path)
        sys.path.append(os.path.dirname(trf_transform_path))
        trf_module = __import__(os.path.splitext(trf_transform)[0])
        if not hasattr(trf_module, 'getTransform'):
            raise Exception("The module %s does not support for dumpArgs" % trf_transform)
        get_transform_method = getattr(trf_module, 'getTransform')
        trf = get_transform_method()
        if not hasattr(trf, 'executors'):
            raise Exception("The module %s does not support for executors list" % trf_transform)
        executor_list = list()
        for executor in trf.executors:
            if executor.name:
                executor_list.append(executor.name)
            if executor.substep:
                executor_list.append(executor.substep)
        del sys.modules[os.path.splitext(trf_transform)[0]]
        return executor_list

    def get_trf_params(self, trf_cache, trf_release, trf_transform, sub_step_list=None, force_dump_args=False,
                       force_ami=False):
        root = '/afs/cern.ch/atlas/software/releases'
        trf_path_t = Template("$root/$base_rel/$cache/$rel/InstallArea/share/bin/$trf")
        trf_release_parts = trf_release.split('.')

        mapping = {'root': root, 'trf': trf_transform}
        list_known_path = list()

        list_known_path.append(trf_transform)

        mapping.update({'base_rel': '.'.join(trf_release_parts[:3]),
                        'rel': trf_release,
                        'cache': trf_cache})
        list_known_path.append(trf_path_t.substitute(mapping))

        if len(trf_release.split('.')) == 5:
            mapping.update({'base_rel': '.'.join(trf_release_parts[:3]),
                            'rel': '.'.join(trf_release_parts[:4]),
                            'cache': 'AtlasProduction'})
            list_known_path.append(trf_path_t.substitute(mapping))

        mapping.update({'base_rel': '.'.join(trf_release_parts[:3]),
                        'rel': '.'.join(trf_release_parts[:3]),
                        'cache': 'AtlasOffline'})
        list_known_path.append(trf_path_t.substitute(mapping))

        mapping.update({'base_rel': '.'.join(trf_release_parts[:3]),
                        'rel': '.'.join(trf_release_parts[:3]),
                        'cache': 'AtlasReconstruction'})
        list_known_path.append(trf_path_t.substitute(mapping))

        mapping.update({'base_rel': '.'.join(trf_release_parts[:3]),
                        'rel': '.'.join(trf_release_parts[:3]),
                        'cache': 'AtlasCore'})
        list_known_path.append(trf_path_t.substitute(mapping))

        mapping.update({'base_rel': '.'.join(trf_release_parts[:3]),
                        'rel': '.'.join(trf_release_parts[:3]),
                        'cache': 'AtlasTrigger'})
        list_known_path.append(trf_path_t.substitute(mapping))

        if len(trf_release.split('.')) == 5:
            mapping.update({'base_rel': '.'.join(trf_release_parts[:3]),
                            'rel': '.'.join(trf_release_parts[:4]),
                            'cache': 'AtlasP1HLT'})
            list_known_path.append(trf_path_t.substitute(mapping))

        trf_params = list()
        trf_transform_path = None

        for path in list_known_path:
            if not os.path.exists(path):
                continue
            with open(path, 'r') as fp:
                params = self._read_trf_params(fp)
                trf_transform_path = path
                if not params:
                    continue
                trf_params.extend(params)
                break

        if (not trf_params) or force_dump_args:
            try:
                trf_params = self._trf_dump_args(list_known_path, trf_transform_path)
            except Exception as ex:
                logger.debug("_trf_dump_args failed: %s" % str(ex))

        if ((not trf_params) or force_ami) and '_tf.' in trf_transform:
            try:
                trf_params = self.ami_get_params(trf_cache, trf_release, trf_transform)
            except Exception as ex:
                logger.exception("ami_get_params failed: %s" % str(ex))

        if not sub_step_list is None:
            # old way from PS1
            if trf_transform.lower() in [e.lower() for e in ['AtlasG4_tf.py', 'Sim_tf.py', 'StoppedParticleG4_tf.py',
                                                             'TrigFTKMergeReco_tf.py', 'Reco_tf.py',
                                                             'FullChain_tf.py', 'Trig_reco_tf.py',
                                                             'OverlayChain_tf.py', 'TrigFTKTM64SM1Un_tf.py',
                                                             'TrigFTKSMUn_Tower22_tf.py', 'Digi_tf.py']]:
                default_sub_steps = ['AODtoRED', 'FTKRecoRDOtoESD', 'all', 'n2n', 'AODtoHIST', 'DQHistogramMerge',
                                     'NTUPtoRED', 'SPSim', 'AODtoTAG', 'AtlasG4Tf', 'ESDtoAOD', 'e2d', 'e2a',
                                     'AODtoDPD',
                                     'sim', 'a2r', 'ESDtoDPD', 'r2e', 'a2d', 'HITtoRDO', 'RAWtoESD', 'default',
                                     'EVNTtoHITS', 'h2r', 'SPGenerator', 'first', 'BSRDOtoRAW', 'b2r', 'OverlayBS',
                                     'RDOFTKCreator', 'AODFTKCreator']
                sub_step_list.extend(default_sub_steps)

        return trf_params

    def _get_proxy(self):
        return VOMSClient().get()

    def sync_ami_projects(self):
        try:
            pyAMI_atlas.api.init()
            ami_projects = pyAMI_atlas.api.list_projects(self.client,
                                                         patterns=['valid%', 'data%', 'mc%', 'user%'],
                                                         fields=['description', 'write_status'])
            project_names = [e.project for e in TProject.objects.all()]
            for ami_project in ami_projects:
                if ami_project['write_status'] != 'valid':
                    continue
                if not ami_project['tag'] in project_names:
                    description = None
                    if str(ami_project['description']) != 'NULL':
                        description = str(ami_project['description'])
                    timestamp = int(time.time())
                    new_project = TProject(project=ami_project['tag'],
                                           status='active',
                                           description=description,
                                           timestamp=timestamp)
                    new_project.save()
                    logger.info('The project \"{0}\" is registered (timestamp={1})'.format(
                        new_project.project,
                        timestamp)
                    )
        except Exception as ex:
            logger.exception('sync_ami_projects, exception occurred: {0}'.format(str(ex)))

    def sync_ami_types(self):
        try:
            pyAMI_atlas.api.init()
            ami_types = pyAMI_atlas.api.list_types(self.client, fields=['description', 'write_status'])
            format_names = [e.name for e in TDataFormat.objects.all()]
            for ami_type in ami_types:
                if ami_type['write_status'] != 'valid':
                    continue
                if not ami_type['name'] in format_names:
                    description = None
                    if str(ami_type['description']) != 'NULL':
                        description = str(ami_type['description'])
                    new_format = TDataFormat(name=ami_type['name'],
                                             description=description)
                    new_format.save()
                    logger.info('The data format \"{0}\" is registered'.format(new_format.name))
        except Exception as ex:
            logger.exception('sync_ami_types, exception occurred: {0}'.format(str(ex)))

    def sync_ami_phys_containers(self):
        try:
            last_created = None
            try:
                last_created = PhysicsContainer.objects.latest('created').created
            except ObjectDoesNotExist:
                pass
            new_datasets = self._ami_list_phys_container(created_after=last_created)
            if new_datasets:
                for dataset in new_datasets:
                    if not PhysicsContainer.objects.filter(pk=dataset['logicalDatasetName']).exists():
                        phys_cont = PhysicsContainer()
                        phys_cont.name = dataset['logicalDatasetName']
                        phys_cont.created = dataset['created']
                        phys_cont.last_modified = dataset['lastModified']
                        phys_cont.username = dataset['createdBy']
                        phys_cont.project = dataset['projectName']
                        phys_cont.data_type = dataset['dataType']
                        phys_cont.run_number = dataset['runNumber']
                        phys_cont.stream_name = dataset['streamName']
                        phys_cont.prod_step = dataset['prodStep']
                        phys_cont.save()
        except Exception as ex:
            logger.exception('sync_ami_phys_containers, exception occurred: {0}'.format(str(ex)))

    def sync_ami_tags(self):
        try:
            last_step_template_id = 0
            try:
                last_step_template_id = ProductionTag.objects.latest('step_template_id').step_template_id
            except ObjectDoesNotExist:
                pass
            result = \
                StepTemplate.objects.filter(id__gt=last_step_template_id).order_by('id').values('id', 'ctag').distinct()
            for step_template in result:
                try:
                    step = StepExecution.objects.filter(step_template__id=step_template['id']).first()
                    if not step:
                        continue
                    task = ProductionTask.objects.filter(step=step).first()
                    if not task or task.id < 400000:
                        continue
                    tag_name = step_template['ctag']
                except ObjectDoesNotExist:
                    continue
                if not ProductionTag.objects.filter(pk=tag_name).exists():
                    tag = ProductionTag()
                    tag.name = tag_name
                    tag.task_id = task.id
                    tag.step_template_id = step_template['id']
                    try:
                        ami_tag = self.get_ami_tag(tag_name)
                        tag.username, tag.created = self.get_ami_tag_owner(tag_name)
                    except Exception:
                        continue
                    tag.trf_name = ami_tag['transformation']
                    tag.trf_cache = ami_tag['SWReleaseCache'].split('_')[0]
                    tag.trf_release = ami_tag['SWReleaseCache'].split('_')[1]
                    tag.tag_parameters = json.dumps(ami_tag)
                    tag.save()
        except Exception as ex:
            logger.exception('sync_ami_tags, exception occurred: {0}'.format(str(ex)))
