import json
from datetime import datetime

from pyworkflow.process import Process, ProcessCompleted, ProcessCanceled, ProcessTimedOut
from pyworkflow.events import Event, DecisionEvent, ActivityEvent, ActivityStartedEvent, SignalEvent, ChildProcessEvent
from pyworkflow.signal import Signal
from pyworkflow.activity import ActivityCompleted, ActivityCanceled, ActivityFailed, ActivityTimedOut, ActivityExecution
from pyworkflow.decision import ScheduleActivity

class AmazonSWFProcess(Process):
    @staticmethod
    def event_from_description(description, related=[]):
        event_type = description['eventType']
        event_dt = datetime.fromtimestamp(description['eventTimestamp'])
        attributes = description.get(event_type[0].lower() + event_type[1:] + 'EventAttributes', {})

        def activity_event_with_result(result):
            scheduled_by = filter(lambda x: x['eventId'] == attributes['scheduledEventId'], related)[0]
            attrs = scheduled_by.get('activityTaskScheduledEventAttributes', None)
            
            try:
                input = json.loads(attrs['input']) if attrs.get('input', None) else None
            except:
                input = attrs.get('input', None)

            activity_execution = ActivityExecution(attrs['activityType']['name'], attrs['activityId'], input)
            if result:
                return ActivityEvent(datetime=event_dt, activity_execution=activity_execution, result=result)
            else:
                return ActivityStartedEvent(datetime=event_dt, activity_execution=activity_execution)

        if event_type == 'ActivityTaskScheduled':
            id = attributes['activityId']
            activity = attributes['activityType']['name']
            input = json.loads(attributes['input']) if attributes.get('input', None) else None
            return DecisionEvent(datetime=event_dt, decision=ScheduleActivity(activity=activity, id=id, input=input))
        elif event_type == 'ActivityTaskStarted':
            return activity_event_with_result(None)
        elif event_type == 'ActivityTaskCompleted':
            result = json.loads(attributes['result']) if 'result' in attributes.keys() else None
            return activity_event_with_result(ActivityCompleted(result=result))
        elif event_type == 'ActivityTaskFailed':
            reason = attributes.get('reason', None)
            details = attributes.get('details', None)
            res = ActivityFailed(reason=reason, details=details)
            return activity_event_with_result(res)
        elif event_type == 'ActivityTaskCanceled':
            details = attributes.get('details', None)
            return activity_event_with_result(ActivityCanceled(details=details))
        elif event_type == 'ActivityTaskTimedOut':
            details = attributes.get('details', None)
            return activity_event_with_result(ActivityTimedOut(details=details))
        elif event_type == 'WorkflowExecutionSignaled':
            try:
                data = json.loads(attributes['input']) if 'input' in attributes.keys() else None
            except:
                data = attributes.get('input', None)
            name = attributes['signalName']
            return SignalEvent(datetime=event_dt, signal=Signal(name=name, data=data))
        elif event_type == 'ChildWorkflowExecutionCompleted':
            result = json.loads(attributes['result']) if 'result' in attributes.keys() else None
            return ChildProcessEvent(datetime=event_dt, process_id=attributes['workflowExecution']['workflowId'], result=ProcessCompleted(result=result))
        elif event_type == 'ChildWorkflowExecutionCanceled':
            details = attributes.get('details', None)
            return ChildProcessEvent(datetime=event_dt, process_id=attributes['workflowExecution']['workflowId'], result=ProcessCanceled(details=details))
        elif event_type == 'ChildWorkflowExecutionTimedOut':
            return ChildProcessEvent(datetime=event_dt, process_id=attributes['workflowExecution']['workflowId'], result=ProcessTimedOut())
        else:
            return None

    @classmethod
    def from_description(cls, description):
        execution_desc = description.get('workflowExecution', None) or description.get('execution', None)
        if not execution_desc:
            return None

        pid = execution_desc['workflowId']

        workflow = description.get('workflowType', {}).get('name', None)
        tags = description.get('tagList', [])

        history = []
        event_descriptions = description.get('events', [])
        for event_description in event_descriptions:
            start_attrs = event_description.get('workflowExecutionStartedEventAttributes', None)
            if start_attrs:
                try:
                    input = json.loads(start_attrs['input'])
                except:
                    input = start_attrs.get('input', None)
                tags = start_attrs['tagList']
                parent = start_attrs.get('parentWorkflowExecution', {}).get('workflowId', None)

            event = cls.event_from_description(event_description, related=event_descriptions)
            if event:
                history.append(event)

        return AmazonSWFProcess(id=pid, workflow=workflow, input=input, tags=tags, history=history, parent=parent)