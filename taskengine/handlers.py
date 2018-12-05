__author__ = 'Dmitry Golubkov'

import collections
from datetime import datetime
from django.utils import timezone
from deftcore.log import Logger, get_exception_string
import deftcore.jedi.client as jedi_client
from taskengine.models import ProductionTask, TRequest, InputRequestList, ProductionDataset, StepExecution
from taskengine.models import TRequestStatus, StepTemplate, TTrfConfig, TConfig
from deftcore.jira import JIRAClient

logger = Logger.get()


class InvalidArgumentError(ValueError):
    pass


class TaskActionHandler(object):
    def parse_jedi_result(self, result):
        return_code = None
        return_info = None
        status_code, status_tuple_or_str = result
        if status_code == 0:
            if isinstance(status_tuple_or_str, collections.Iterable):
                status_list = [e for e in status_tuple_or_str]
                if len(status_list) > 1:
                    return_code = status_list[0]
                    return_info = status_list[1]
                else:
                    return_code = status_list[0]
                    return_info = None
            else:
                return_code = status_tuple_or_str
                return_info = None
        elif status_code == 255:
            return_code = None
            return_info = str(status_tuple_or_str)
        return {'jedi_info': {'status_code': status_code, 'return_code': return_code, 'return_info': return_info}}

    def abort_task(self, task_id):
        result = jedi_client.killTask(task_id)
        return self.parse_jedi_result(result)

    def finish_task(self, task_id, soft=False):
        result = jedi_client.finishTask(task_id, soft)
        return self.parse_jedi_result(result)

    def reassign_task(self, task_id, site=None, cloud=None, nucleus=None, mode=None):
        if site or site == '':
            result = jedi_client.reassignTaskToSite(task_id, site, mode=mode)
        elif cloud or cloud == '':
            result = jedi_client.reassignTaskToCloud(task_id, cloud, mode=mode)
        elif nucleus or nucleus == '':
            result = jedi_client.reassignTaskToNucleus(task_id, nucleus, mode=mode)
        else:
            raise InvalidArgumentError()
        return self.parse_jedi_result(result)

    def reassign_jobs(self, task_id, for_pending, first_submission):
        result = jedi_client.reassignJobs([task_id, ], forPending=for_pending, firstSubmission=first_submission)
        return self.parse_jedi_result(result)

    def change_task_priority(self, task_id, priority):
        result = jedi_client.changeTaskPriority(task_id, priority)
        return self.parse_jedi_result(result)

    def change_task_ram_count(self, task_id, ram_count):
        result = jedi_client.changeTaskRamCount(task_id, ram_count)
        return self.parse_jedi_result(result)

    def change_task_wall_time(self, task_id, wall_time):
        result = jedi_client.changeTaskWalltime(task_id, wall_time)
        return self.parse_jedi_result(result)

    def change_task_cpu_time(self, task_id, cpu_time):
        result = jedi_client.changeTaskCputime(task_id, cpu_time)
        return self.parse_jedi_result(result)

    def change_task_split_rule(self, task_id, rule_name, rule_value):
        result = jedi_client.changeTaskSplitRule(task_id, rule_name, rule_value)
        return self.parse_jedi_result(result)

    def change_task_attribute(self, task_id, attr_name, attr_value):
        result = jedi_client.changeTaskAttribute(task_id, attr_name, attr_value)
        return self.parse_jedi_result(result)

    def retry_task(self, task_id, discard_events):
        result = jedi_client.retryTask(task_id, verbose=False, discardEvents=discard_events)
        return self.parse_jedi_result(result)

    def reload_input(self, task_id):
        result = jedi_client.reloadInput(task_id, verbose=False)
        return self.parse_jedi_result(result)

    def pause_task(self, task_id):
        result = jedi_client.pauseTask(task_id, verbose=False)
        return self.parse_jedi_result(result)

    def resume_task(self, task_id):
        result = jedi_client.resumeTask(task_id, verbose=False)
        return self.parse_jedi_result(result)

    def reassign_task_to_share(self, task_id, share):
        result = jedi_client.reassignShare([task_id, ], share)
        return self.parse_jedi_result(result)

    def trigger_task_brokerage(self, task_id):
        result = jedi_client.triggerTaskBrokerage(task_id)
        return self.parse_jedi_result(result)

    def avalanche_task(self, task_id):
        result = jedi_client.avalancheTask(task_id)
        return self.parse_jedi_result(result)

    def increase_attempt_number(self, task_id, increment):
        result = jedi_client.increaseAttemptNr(task_id, increment)
        return self.parse_jedi_result(result)

    def abort_unfinished_jobs(self, task_id, code):
        result = jedi_client.killUnfinishedJobs(task_id, code=code)
        return {'jedi_info': {'status_code': result[0], 'return_code': None, 'return_info': None}}

    def add_task_comment(self, task_id, comment_body):
        if not task_id:
            return
        try:
            task = ProductionTask.objects.get(id=task_id)
        except ProductionTask.DoesNotExist:
            logger.info('The task {0} is not found'.format(task_id))
            return
        try:
            if task.reference:
                client = JIRAClient()
                client.authorize()
                client.add_issue_comment(task.reference, comment_body)
        except Exception:
            logger.info('add_task_comment, exception occurred: {0}'.format(get_exception_string()))

    def _parse_pp_command(self, pp_command_str):
        pp_command = dict()
        if pp_command_str:
            for e in pp_command_str.split(';'):
                if not e:
                    continue
                key = e.split(':')[0].replace(' ', '')
                values = e.split(':')[1].replace(' ', '').split(',')
                pp_command.update({key: values})
        return pp_command

    def _construct_pp_command(self, pp_command):
        pp_command_list = list()
        for key in pp_command.keys():
            if pp_command[key]:
                pp_command_list.append('{0} : {1};'.format(key, ', '.join(pp_command[key])))
        return ''.join(pp_command_list)

    def clean_task_carriages(self, task_id, output_formats):
        is_updated = False
        task = ProductionTask.objects.get(id=task_id)
        # 'trainCC : DAOD, ESD; merge : HITS;'
        pp_command = self._parse_pp_command(task.postproduction)
        if 'trainCC' in pp_command.keys():
            for e in output_formats.split('.'):
                if not e in pp_command['trainCC']:
                    pp_command['trainCC'].append(e)
                    is_updated = True
        else:
            pp_command['trainCC'] = output_formats.split('.')
            is_updated = True
        if is_updated:
            task.postproduction = self._construct_pp_command(pp_command)
            task.pptimestamp = timezone.now()
            task.save()
        return {'result': task.postproduction}

    def kill_job(self, job_id, code=9, keep_unmerged=False):
        result = jedi_client.killJobs([job_id, ], code=code, keepUnmerged=keep_unmerged)
        return self.parse_jedi_result(result)

    def set_job_debug_mode(self, job_id, debug_mode):
        result = jedi_client.setDebugMode(job_id, debug_mode)
        # FIXME
        status_code, return_info = result
        return {'jedi_info': {'status_code': status_code, 'return_code': None, 'return_info': return_info}}

    def set_ttcr(self, offsets):
        TConfig.set_ttcr(offsets)
        return {'result': 'Success'}

    def set_ttcj(self, ttcj_dict):
        for task_id in ttcj_dict.keys():
            task = ProductionTask.objects.get(id=task_id)
            task.ttcj_timestamp = datetime.fromtimestamp(ttcj_dict[task_id])
            task.ttcj_update_time = timezone.now()
            task.save()
        return {'result': 'Success'}

    # FIXME: to delete
    def _fill_template(self, step_name, tag, priority, formats=None, ram=None):
        STEP_FORMAT = {'Evgen': 'EVNT', 'Simul': 'HITS', 'Merge': 'HITS', 'Rec TAG': 'TAG',
                       'Atlf Merge': 'AOD', 'Atlf TAG': 'TAG'
                       }
        st = None
        try:
            if not step_name:
                if (not formats) and (not ram):
                    st = StepTemplate.objects.all().filter(ctag=tag)[0]
                if (not formats) and (ram):
                    st = StepTemplate.objects.all().filter(ctag=tag, memory=ram)[0]
                if (formats) and (not ram):
                    st = StepTemplate.objects.all().filter(ctag=tag, output_formats=formats)[0]
                if (formats) and (ram):
                    st = StepTemplate.objects.all().filter(ctag=tag, output_formats=formats, memory=ram)[0]
            else:
                if (not formats) and (not ram):
                    st = StepTemplate.objects.all().filter(ctag=tag, step=step_name)[0]
                if (not formats) and (ram):
                    st = StepTemplate.objects.all().filter(ctag=tag, memory=ram, step=step_name)[0]
                if (formats) and (not ram):
                    st = StepTemplate.objects.all().filter(ctag=tag, output_formats=formats, step=step_name)[0]
                if (formats) and (ram):
                    st = \
                        StepTemplate.objects.all().filter(ctag=tag, output_formats=formats, memory=ram, step=step_name)[
                            0]
        except:
            pass
        finally:
            if st:
                if (st.status == 'Approved') or (st.status == 'dummy'):
                    return st

            trtf = TTrfConfig.objects.all().filter(tag=tag.strip()[0], cid=int(tag.strip()[1:]))
            if trtf:
                tr = trtf[0]
                if (formats):
                    output_formats = formats
                else:
                    output_formats = tr.formats
                if (ram):
                    memory = ram
                else:
                    memory = int(tr.memory)
                if not step_name:
                    step_name = tr.step
                if st:
                    st.status = 'Approved'
                    st.output_formats = output_formats
                    st.memory = memory
                    st.cpu_per_event = int(tr.cpu_per_event)
                else:
                    st = StepTemplate.objects.create(step=step_name, def_time=timezone.now(), status='Approved',
                                                     ctag=tag, priority=priority,
                                                     cpu_per_event=int(tr.cpu_per_event), memory=memory,
                                                     output_formats=output_formats, trf_name=tr.trf,
                                                     lparams='', vparams='', swrelease=tr.trfv)
                st.save()
                # _logger.debug('Created step template: %i' % st.id)
                return st
            else:
                if (not step_name) or (not tag):
                    raise ValueError("Can't create an empty step")
                else:
                    if st:
                        return st
                    output_formats = STEP_FORMAT.get(step_name, '')
                    if formats:
                        output_formats = formats
                    memory = 0
                    if ram:
                        memory = ram
                    st = StepTemplate.objects.create(step=step_name, def_time=timezone.now(), status='dummy',
                                                     ctag=tag, priority=0,
                                                     cpu_per_event=0, memory=memory,
                                                     output_formats=output_formats, trf_name='',
                                                     lparams='', vparams='', swrelease='')
                    st.save()
                    return st

    # FIXME: to delete
    def _fill_dataset(self, ds):
        dataset = None
        try:
            dataset = ProductionDataset.objects.all().filter(name=ds)[0]
        except:
            pass
        finally:
            if dataset:
                return dataset
            else:
                dataset = ProductionDataset.objects.create(name=ds, files=-1, timestamp=timezone.now())
                dataset.save()
                return dataset

    # FIXME: to delete
    def _set_step_task_config(self, step, update_dict):
        import json

        if not step.task_config:
            step.task_config = ''
            current_dict = {}
        else:
            current_dict = json.loads(step.task_config)
        current_dict.update(update_dict)
        step.task_config = json.dumps(current_dict)

    # FIXME: to delete
    def _save_step_with_current_time(self, step, *args, **kwargs):
        if not step.step_def_time:
            step.step_def_time = timezone.now()
        if step.status == 'Approved':
            if not step.step_appr_time:
                step.step_appr_time = timezone.now()
        step.save(*args, **kwargs)

    # FIXME: to delete
    def _save_rs_with_current_time(self, rs, *args, **kwargs):
        if not rs.timestamp:
            rs.timestamp = timezone.now()
        rs.save(*args, **kwargs)

    # FIXME: to delete
    def create_slice_tier0(self, slice_dict, steps_list):
        TASK_CONFIG_PARAMS = ['input_format', 'nEventsPerJob', 'token', 'merging_tag',
                              'nFilesPerMergeJob', 'nGBPerMergeJob', 'nMaxFilesPerMergeJob', 'project_mode',
                              'nFilesPerJob', 'nGBPerJob', 'maxAttempt']

        def make_new_slice(slice_dict, last_request):
            if InputRequestList.objects.filter(request=last_request).count() == 0:
                new_slice_number = 0
            else:
                new_slice_number = \
                    (InputRequestList.objects.filter(request=last_request).order_by('-slice')[0]).slice + 1
            new_slice = InputRequestList()
            if slice_dict.get('dataset', ''):
                dataset = self._fill_dataset(slice_dict['dataset'])
                new_slice.input_dataset = dataset.name
            else:
                raise ValueError('Dataset has to be defined')
            new_slice.input_events = -1
            new_slice.slice = new_slice_number
            new_slice.request = last_request
            new_slice.comment = slice_dict.get('comment', '')
            new_slice.priority = slice_dict.get('priority', 950)
            new_slice.brief = ' '
            new_slice.save()
            return new_slice

        last_request = (TRequest.objects.filter(request_type='TIER0').order_by('-id'))[0]

        parent = None
        output_slice_step = {}
        current_slice = make_new_slice(slice_dict, last_request)
        slice_last_step = {}

        for step_dict in steps_list:
            new_step = StepExecution()
            new_step.request = last_request
            # new_step.slice = new_slice

            new_step.input_events = -1
            if step_dict.get('ctag', ''):
                ctag = step_dict.get('ctag', '')
            else:
                raise ValueError('Ctag has to be defined for step')
            if step_dict.get('input_format', ''):
                if step_dict['input_format'] not in output_slice_step:
                    raise ValueError('no parent step found for %s' % step_dict['input_format'])
                else:
                    if slice_last_step[output_slice_step[step_dict['input_format']][0].slice] != \
                            output_slice_step[step_dict['input_format']][1]:
                        current_slice = make_new_slice(slice_dict, last_request)
                    else:
                        current_slice = output_slice_step[step_dict['input_format']][0]
                    parent = output_slice_step[step_dict['input_format']][1]
            new_step.slice = current_slice
            if step_dict.get('output_formats', ''):
                output_formats = step_dict.get('output_formats', '')
            else:
                raise ValueError('output_formats has to be defined for step')
            new_step.priority = step_dict.get('priority', 950)
            memory = step_dict.get('memory', 0)
            new_step.step_template = self._fill_template('Reco', ctag, new_step.priority, output_formats, memory)
            if ('nFilesPerJob' not in step_dict) and ('nGBPerJob' not in step_dict):
                raise ValueError('nFilesPerJob or nGBPerJob have to be defined')
            for parameter in TASK_CONFIG_PARAMS:
                if parameter in step_dict:
                    self._set_step_task_config(new_step, {parameter: step_dict[parameter]})
            if parent:
                new_step.step_parent_id = parent.id
            new_step.status = 'Approved'
            self._save_step_with_current_time(new_step)
            if not parent:
                new_step.step_parent_id = new_step.id
                new_step.save()
            for output_format in output_formats.split('.'):
                output_slice_step[output_format] = (current_slice, new_step)
            parent = new_step
            slice_last_step[current_slice.slice] = new_step
        last_request.status = 'approved'
        last_request.save()
        request_status = TRequestStatus(request=last_request, comment='Request approved by Tier0', owner='tier0',
                                        status=last_request.status)
        self._save_rs_with_current_time(request_status)
        return {'reqID': int(last_request.id)}
