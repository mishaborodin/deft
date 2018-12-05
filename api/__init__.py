__author__ = 'Dmitry Golubkov'

import json
import threading
from django.utils import timezone
from deftcore.helpers import Singleton
from deftcore.log import Logger, get_exception_string
from taskengine.protocol import Protocol, TaskStatus

logger = Logger().get()


class ApiServer(object):
    __metaclass__ = Singleton

    def _process_api_request(self, request):
        try:
            from taskengine.models import ProductionTask
            from taskengine.handlers import TaskActionHandler
            handler = TaskActionHandler()
            if request.action == request.ACTION_TEST:
                status = {'result': "test"}
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=status)
            elif request.action == request.ACTION_CLONE_TASK:
                raise NotImplementedError()
            elif request.action == request.ACTION_ABORT_TASK:
                body = json.loads(request.body)
                task_id = int(body['task_id'])
                handler_status = handler.abort_task(task_id)
                try:
                    jedi_info = handler_status['jedi_info']
                    if jedi_info['status_code'] == 0 and jedi_info['return_code'] == 0:
                        task = ProductionTask.objects.get(id=task_id)
                        task.status = Protocol().TASK_STATUS[TaskStatus.TOABORT]
                        task.save()
                except:
                    logger.exception("Exception occurred: %s" % get_exception_string())
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
                handler.add_task_comment(task_id, request.create_default_task_comment(body))
            elif request.action == request.ACTION_FINISH_TASK:
                body = json.loads(request.body)
                task_id = int(body['task_id'])
                soft = bool(body.get('soft'))
                handler_status = handler.finish_task(task_id, soft)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
                handler.add_task_comment(task_id, request.create_default_task_comment(body))
            elif request.action == request.ACTION_OBSOLETE_TASK:
                body = json.loads(request.body)
                task_id = int(body['task_id'])
                task = ProductionTask.objects.get(id=task_id)
                task.status = Protocol().TASK_STATUS[TaskStatus.OBSOLETE]
                task.timestamp = timezone.now()
                task.save()
                request.set_status(request.STATUS_RESULT_SUCCESS)
                handler.add_task_comment(task_id, request.create_default_task_comment(body))
            elif request.action == request.ACTION_OBSOLETE_ENTITY:
                body = json.loads(request.body)
                task_id_list = [int(e) for e in str(body['tasks']).split(',')]
                is_force = bool(body.get('force', None))
                tasks = ProductionTask.objects.filter(id__in=task_id_list)
                is_chain = len(tasks) > 1
                for task in tasks:
                    task.status = Protocol().TASK_STATUS[TaskStatus.OBSOLETE]
                    task.timestamp = timezone.now()
                    if is_chain:
                        task.pp_flag = 2
                        if is_force:
                            task.pp_grace_period = 0
                        else:
                            task.pp_grace_period = 48
                    else:
                        if is_force:
                            task.pp_flag = 1
                            task.pp_grace_period = 0
                        else:
                            task.pp_flag = 0
                            task.pp_grace_period = 48
                    task.save()
                    request.set_status(request.STATUS_RESULT_SUCCESS)
                    handler.add_task_comment(task.id, request.create_default_task_comment(body))
            elif request.action == request.ACTION_REASSIGN_TASK:
                body = json.loads(request.body)
                task_id = int(body['task_id'])
                site = body.get('site', None)
                cloud = body.get('cloud', None)
                nucleus = body.get('nucleus', None)
                mode = body.get('mode', None)
                handler_status = handler.reassign_task(task_id, site, cloud, nucleus, mode=mode)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
                handler.add_task_comment(task_id, request.create_default_task_comment(body))
            elif request.action == request.ACTION_REASSIGN_JOBS:
                body = json.loads(request.body)
                task_id = int(body['task_id'])
                for_pending = bool(body.get('for_pending', None))
                first_submission = bool(body.get('first_submission', None))
                handler_status = handler.reassign_jobs(task_id, for_pending, first_submission)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
                handler.add_task_comment(task_id, request.create_default_task_comment(body))
            elif request.action == request.ACTION_CHANGE_TASK_PRIORITY:
                body = json.loads(request.body)
                task_id = int(body['task_id'])
                priority = int(body['priority'])
                handler_status = handler.change_task_priority(task_id, priority)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
                handler.add_task_comment(task_id, request.create_default_task_comment(body))
            elif request.action == request.ACTION_CHANGE_TASK_RAM_COUNT:
                body = json.loads(request.body)
                task_id = int(body['task_id'])
                ram_count = int(body['ram_count'])
                handler_status = handler.change_task_ram_count(task_id, ram_count)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
                handler.add_task_comment(task_id, request.create_default_task_comment(body))
            elif request.action == request.ACTION_CHANGE_TASK_WALL_TIME:
                body = json.loads(request.body)
                task_id = int(body['task_id'])
                wall_time = int(body['wall_time'])
                handler_status = handler.change_task_wall_time(task_id, wall_time)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
                handler.add_task_comment(task_id, request.create_default_task_comment(body))
            elif request.action == request.ACTION_CHANGE_TASK_CPU_TIME:
                body = json.loads(request.body)
                task_id = int(body['task_id'])
                cpu_time = int(body['cpu_time'])
                handler_status = handler.change_task_cpu_time(task_id, cpu_time)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
                handler.add_task_comment(task_id, request.create_default_task_comment(body))
            elif request.action == request.ACTION_CHANGE_TASK_SPLIT_RULE:
                body = json.loads(request.body)
                task_id = int(body['task_id'])
                rule_name = body['rule_name']
                rule_value = body['rule_value']
                handler_status = handler.change_task_split_rule(task_id, rule_name, rule_value)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
                handler.add_task_comment(task_id, request.create_default_task_comment(body))
            elif request.action == request.ACTION_CHANGE_TASK_ATTRIBUTE:
                body = json.loads(request.body)
                task_id = int(body['task_id'])
                attr_name = body['attr_name']
                attr_value = body['attr_value']
                handler_status = handler.change_task_attribute(task_id, attr_name, attr_value)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
                handler.add_task_comment(task_id, request.create_default_task_comment(body))
            elif request.action == request.ACTION_RETRY_TASK:
                body = json.loads(request.body)
                task_id = int(body['task_id'])
                discard_events = bool(body.get('discard_events', False))
                handler_status = handler.retry_task(task_id, discard_events)
                try:
                    jedi_info = handler_status['jedi_info']
                    if jedi_info['status_code'] == 0 and jedi_info['return_code'] == 0:
                        task = ProductionTask.objects.get(id=task_id)
                        task.status = Protocol().TASK_STATUS[TaskStatus.TORETRY]
                        task.save()
                except:
                    logger.exception("Exception occurred: %s" % get_exception_string())
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
                handler.add_task_comment(task_id, request.create_default_task_comment(body))
            elif request.action == request.ACTION_PAUSE_TASK:
                body = json.loads(request.body)
                task_id = int(body['task_id'])
                handler_status = handler.pause_task(task_id)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
                handler.add_task_comment(task_id, request.create_default_task_comment(body))
            elif request.action == request.ACTION_RESUME_TASK:
                body = json.loads(request.body)
                task_id = int(body['task_id'])
                handler_status = handler.resume_task(task_id)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
                handler.add_task_comment(task_id, request.create_default_task_comment(body))
            elif request.action == request.ACTION_REASSIGN_TASK_TO_SHARE:
                body = json.loads(request.body)
                task_id = int(body['task_id'])
                share = body.get('share', '')
                handler_status = handler.reassign_task_to_share(task_id, share)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
                handler.add_task_comment(task_id, request.create_default_task_comment(body))
            elif request.action == request.ACTION_TRIGGER_TASK_BROKERAGE:
                body = json.loads(request.body)
                task_id = int(body['task_id'])
                handler_status = handler.trigger_task_brokerage(task_id)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
                handler.add_task_comment(task_id, request.create_default_task_comment(body))
            elif request.action == request.ACTION_AVALANCHE_TASK:
                body = json.loads(request.body)
                task_id = int(body['task_id'])
                handler_status = handler.avalanche_task(task_id)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
                handler.add_task_comment(task_id, request.create_default_task_comment(body))
            elif request.action == request.ACTION_INCREASE_ATTEMPT_NUMBER:
                body = json.loads(request.body)
                task_id = int(body['task_id'])
                increment = int(body['increment'])
                handler_status = handler.increase_attempt_number(task_id, increment)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
                handler.add_task_comment(task_id, request.create_default_task_comment(body))
            elif request.action == request.ACTION_ABORT_UNFINISHED_JOBS:
                body = json.loads(request.body)
                task_id = int(body['task_id'])
                code = body.get('code', 9)
                handler_status = handler.abort_unfinished_jobs(task_id, code)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
                handler.add_task_comment(task_id, request.create_default_task_comment(body))
            elif request.action == request.ACTION_ADD_TASK_COMMENT:
                body = json.loads(request.body)
                task_id = int(body['task_id'])
                comment_body = body['comment_body']
                handler_status = handler.add_task_comment(task_id, comment_body)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
            elif request.action == request.ACTION_CREATE_SLICE_TIER0:
                body = json.loads(request.body)
                slice_dict = body['slice_dict']
                steps_list = body['steps_list']
                handler_status = handler.create_slice_tier0(slice_dict, steps_list)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
            elif request.action == request.ACTION_CLEAN_TASK_CARRIAGES:
                body = json.loads(request.body)
                task_id = body['task_id']
                output_formats = body['output_formats']
                handler_status = handler.clean_task_carriages(task_id, output_formats)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
                handler.add_task_comment(task_id, request.create_default_task_comment(body))
            elif request.action == request.ACTION_KILL_JOB:
                body = json.loads(request.body)
                task_id = body['task_id']
                job_id = body['job_id']
                code = body.get('code', 9)
                keep_unmerged = bool(body.get('keep_unmerged', False))
                handler_status = handler.kill_job(job_id, code=code, keep_unmerged=keep_unmerged)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
                status_code = handler_status['jedi_info']['status_code']
                body.update({'status_code': status_code})
                handler.add_task_comment(task_id, request.create_default_task_comment(body))
            elif request.action == request.ACTION_SET_JOB_DEBUG_MODE:
                body = json.loads(request.body)
                task_id = body['task_id']
                job_id = body['job_id']
                debug_mode = body['debug_mode']
                handler_status = handler.set_job_debug_mode(job_id, debug_mode)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
                status_code = handler_status['jedi_info']['status_code']
                body.update({'status_code': status_code})
                handler.add_task_comment(task_id, request.create_default_task_comment(body))
            elif request.action == request.ACTION_SET_TTCR:
                body = json.loads(request.body)
                ttcr_dict = body['ttcr_dict']
                handler_status = handler.set_ttcr(ttcr_dict)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
            elif request.action == request.ACTION_SET_TTCJ:
                body = json.loads(request.body)
                ttcj_dict = body['ttcj_dict']
                handler_status = handler.set_ttcj(ttcj_dict)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
            elif request.action == request.ACTION_RELOAD_INPUT:
                body = json.loads(request.body)
                task_id = body['task_id']
                handler_status = handler.reload_input(task_id)
                request.set_status(request.STATUS_RESULT_SUCCESS, data_dict=handler_status)
                handler.add_task_comment(task_id, request.create_default_task_comment(body))
            else:
                raise Exception("Invalid action: %s" % request.action)
        except Exception:
            logger.exception("Exception occurred: %s" % get_exception_string())
            if request:
                request.set_status(request.STATUS_RESULT_EXCEPTION, exception=get_exception_string())

    def process_request(self, request):
        threading.Thread(target=self._process_api_request, args=(request,)).start()
