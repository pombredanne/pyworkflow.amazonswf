__version__ = '0.1.0'
__author__ = 'Willem Bult'
__email__ = 'willem.bult@gmail.com'

import json
import uuid
import time
import boto.swf
import re

from boto.exception import SWFResponseError
from datetime import datetime, timedelta
from itertools import imap

from pyworkflow.backend import Backend
from pyworkflow.exceptions import UnknownDecisionException, UnknownActivityException
from pyworkflow.defaults import Defaults

from process import AmazonSWFProcess, ActivityCompleted, ActivityFailed, ActivityCanceled
from task import decision_task_from_description, activity_task_from_description
from decision import AmazonSWFDecision

def uncamelcase(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

class AmazonSWFConfiguration(dict):

    def config_workflow(self, name, conf):
        self['%sWorkflow' % name] = conf

    def config_activity(self, name, conf):
        self['%sActivity' % name] = conf

    def _export(self, conf, camelcase=True, prepend=''):
        if prepend:
            conf = dict(('%s%s%s' % (prepend, k[0].upper(), k[1:]), v) for (k,v) in conf.items())

        if not camelcase:
            conf = dict( (uncamelcase(k), v) for (k,v) in conf.items() )

        return conf

    def for_workflow(self, name, camelcase=True, prepend=''):
        return self._export(self['%sWorkflow' % name], camelcase, prepend)

    def for_activity(self, name, camelcase=True, prepend=''):
        return self._export(self['%sActivity' % name], camelcase, prepend)


        
class AmazonSWFBackend(Backend):
    
    @staticmethod
    def _get_region(name):
        return next((region for region in boto.swf.regions() if region.name == name), None )

    def __init__(self, access_key_id, secret_access_key, region='us-east-1', domain='default'):
        self.domain = domain
        self._swf = boto.swf.layer1.Layer1(access_key_id, secret_access_key, region=self._get_region(region))
        self._config = AmazonSWFConfiguration()

    def _consume_until_exhaustion(self, request_fn):
        next_page_token = None
        while True:
            response = request_fn(next_page_token)
            yield response

            next_page_token = response.get('nextPageToken', None)
            if not next_page_token:
                break
    
    def register_workflow(self, name, category=Defaults.DECISION_CATEGORY,
        timeout=Defaults.WORKFLOW_TIMEOUT, decision_timeout=Defaults.DECISION_TIMEOUT):

        self._config.config_workflow(name, {
            'taskList': {'name': str(category)},
            'childPolicy': 'ABANDON',
            'executionStartToCloseTimeout': str(timeout),
            'taskStartToCloseTimeout': str(decision_timeout)
        })

        try:
            self._swf.describe_workflow_type(self.domain, name, "1.0")
        except:
            # Workflow type not registered yet
            config = self._config.for_workflow(name, camelcase=False, prepend='default')
            del config['default_task_list'] # don't need this in there
            self._swf.register_workflow_type(self.domain, name, "1.0", task_list=category, **config)

    def register_activity(self, name, category=Defaults.ACTIVITY_CATEGORY, 
        scheduled_timeout=Defaults.ACTIVITY_SCHEDULED_TIMEOUT, 
        execution_timeout=Defaults.ACTIVITY_EXECUTION_TIMEOUT, 
        heartbeat_timeout=Defaults.ACTIVITY_HEARTBEAT_TIMEOUT):

        self._config.config_activity(name, {
            'taskList': {'name': str(category)},
            'heartbeatTimeout': str(heartbeat_timeout),
            'scheduleToStartTimeout': str(scheduled_timeout),
            'scheduleToCloseTimeout': str(scheduled_timeout+execution_timeout),
            'startToCloseTimeout': str(execution_timeout)
        })

        try:
            self._swf.describe_activity_type(self.domain, name, "1.0")
        except:
            config = self._config.for_activity(name, camelcase=False, prepend='defaultTask')
            del config['default_task_task_list'] # don't need this in there
            self._swf.register_activity_type(self.domain, name, "1.0", task_list=category, **config)
    
    def start_process(self, process):
        if process.id is not None:
            raise ValueError('AmazonSWF does not support manually assigned ids on a process. Process.id should be None.')

        if process.tags and len(process.tags) > 5:
            raise ValueError('AmazonSWF supports a maximum of 5 tags per process')

        if process.parent is not None:
            raise ValueError('AmazonSWF does not support starting child tasks directly. Use StartChildProcess decision instead.')

        config = self._config.for_workflow(process.workflow, camelcase=False)
        if not config:
            raise ValueError('Unknown workflow in process.workflow. Call register_workflow first.')

        config['task_list'] = config['task_list']['name'] # should extract for boto

        workflow_id = str(uuid.uuid4())
        ret = self._swf.start_workflow_execution(
            self.domain, workflow_id, process.workflow, "1.0",
            input=json.dumps(process.input),
            tag_list=process.tags,
            **config)

        run_id = ret['runId']
        return '%s:%s' % (workflow_id, run_id)
        
    def signal_process(self, process_or_id, signal, data=None):
        pid = process_or_id.id if hasattr(process_or_id, 'id') else process_or_id
        self._swf.signal_workflow_execution(
            self.domain, signal, pid.split(':')[0],
            input=json.dumps(data))

    def cancel_process(self, process_or_id, details=None):
        pid = process_or_id.id if hasattr(process_or_id, 'id') else process_or_id
        self._swf.terminate_workflow_execution(
            self.domain, pid.split(':')[0], 
            details=details)

    def heartbeat_activity_task(self, task):
        self._swf.record_activity_task_heartbeat(task.context['token'])

    def complete_decision_task(self, task, decisions):
        if not type(decisions) is list:
            decisions = [decisions]
        descriptions = [AmazonSWFDecision(d, self._config).description for d in decisions]

        try:
            self._swf.respond_decision_task_completed(task.context['token'], 
                decisions=descriptions,
                execution_context=None)
        except SWFResponseError, e:
            if e.body.get('__type', None) == 'com.amazonaws.swf.base.model#UnknownResourceFault':
                raise UnknownDecisionException()
            else:
                raise e

    def complete_activity_task(self, task, result=None):
        try:
            if isinstance(result, ActivityCompleted):
                self._swf.respond_activity_task_completed(task.context['token'], result=json.dumps(result.result))
            elif isinstance(result, ActivityCanceled):
                self._swf.respond_activity_task_canceled(task.context['token'], details=result.details)
            elif isinstance(result, ActivityFailed):
                self._swf.respond_activity_task_failed(task.context['token'], details=result.details, reason=result.reason)
            else:
                raise ValueError('Expected result of type in [ActivityCompleted, ActivityCanceled, ActivityFailed]')
        except SWFResponseError, e:
            if e.body.get('__type', None) == 'com.amazonaws.swf.base.model#UnknownResourceFault':
                raise UnknownActivityException()
            else:
                raise e

    def _workflow_execution_history(self, description):
        run_id = description['execution']['runId']
        workflow_id = description['execution']['workflowId']

        # exhaustively query execution history using next_page_token
        response_iter = self._consume_until_exhaustion(
            lambda token: self._swf.get_workflow_execution_history(self.domain, run_id, workflow_id, next_page_token=token),
        )

        return {'events': [ev for response in response_iter for ev in response.get('events', [])]}

    def _process_from_description(self, description):
        # get and fill in event history
        history = self._workflow_execution_history(description)
        description.update(history)
        process = AmazonSWFProcess.from_description(description)
        return process

    def process_by_id(self, process_id):
        workflow_id, run_id = process_id.split(':')
        description = self._swf.describe_workflow_execution(self.domain, run_id, workflow_id)
        return self._process_from_description(description['executionInfo'])
    
    def processes(self, workflow=None, tag=None):
        if workflow and tag:
            raise Exception('Amazon SWF does not support filtering on "workflow" and "tag" at the same time')

        # Max lifetime of workflow executions in SWF is 1 year
        after_date = datetime.now() - timedelta(days=365)
        oldest_timestamp = time.mktime(after_date.timetuple())

        response_iter = self._consume_until_exhaustion(
            lambda token: self._swf.list_open_workflow_executions(self.domain, oldest_timestamp, workflow_name=workflow, tag=tag if input else None, next_page_token=token)
        )

        return imap(lambda d: self._process_from_description(d), (d for response in response_iter for d in response['executionInfos']))

    def poll_activity_task(self, category=Defaults.ACTIVITY_CATEGORY, identity=None):
        description = self._swf.poll_for_activity_task(self.domain, category, identity=identity)
        return activity_task_from_description(description) if description else None

    def poll_decision_task(self, category=Defaults.DECISION_CATEGORY, identity=None):
        response_iter = self._consume_until_exhaustion(
            lambda token: self._swf.poll_for_decision_task(self.domain, category, identity=identity, next_page_token=token),
        )

        description = next(response_iter, None)
        if description and description.get('events',None):
            description['events'] += [ev for response in response_iter for ev in response.get('events', [])]
            return decision_task_from_description(description)
        else:
            return None