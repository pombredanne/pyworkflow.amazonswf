import json
from datetime import datetime

from pyworkflow.process import Process, ProcessCompleted, ProcessCanceled, ProcessTimedOut
from pyworkflow.events import Event, DecisionStartedEvent, DecisionEvent, ActivityEvent, ActivityStartedEvent, SignalEvent, ChildProcessEvent, TimerEvent, ProcessStartedEvent
from pyworkflow.signal import Signal
from pyworkflow.activity import ActivityCompleted, ActivityCanceled, ActivityFailed, ActivityTimedOut, ActivityExecution
from pyworkflow.decision import ScheduleActivity, StartChildProcess, Timer

class AmazonSWFProcess(Process):

    @classmethod
    def _related_event(cls, related, event_id):
        return filter(lambda x: x['eventId'] == event_id, related)[0]

    @classmethod
    def _attributes(cls, event):
        event_type = event['eventType']
        return event.get(event_type[0].lower() + event_type[1:] + 'EventAttributes', {})
        
    @classmethod
    def _decision_event(cls, event, related):
        event_id = event['eventId']
        event_dt = datetime.fromtimestamp(event['eventTimestamp'])
        event_type = event['eventType']
        attributes = cls._attributes(event)

        completed_event = cls._related_event(related, attributes['decisionTaskCompletedEventId'])
        started_event_id = completed_event['decisionTaskCompletedEventAttributes']['startedEventId']
    
        inp = json.loads(attributes['input']) if attributes.get('input', None) else None

        if event_type == 'ActivityTaskScheduled':
            activity_id = attributes['activityId']
            activity = attributes['activityType']['name']
            decision = ScheduleActivity(activity=activity, id=activity_id, input=inp)

        elif event_type == 'StartChildWorkflowExecutionInitiated':
            process = Process(workflow=attributes['workflowType']['name'], input=inp, tags=attributes['tagList'])
            decision = StartChildProcess(process=process)
        
        elif event_type == 'TimerStarted':
            decision = Timer(delay=int(attributes['startToFireTimeout']), data=json.loads(attributes['control']))
        
        # logical order is start of decision, everything after needs to be considered in next decision
        return (started_event_id, DecisionEvent(datetime=event_dt, decision=decision))

    @classmethod
    def _activity_event(cls, event, related):
        event_id = event['eventId']
        event_dt = datetime.fromtimestamp(event['eventTimestamp'])
        event_type = event['eventType']
        attributes = cls._attributes(event)
        
        def event_with_result(result):
            scheduled_by = cls._related_event(related, attributes['scheduledEventId'])
            attrs = scheduled_by.get('activityTaskScheduledEventAttributes', None)
            
            try:
                inp = json.loads(attrs['input']) if attrs.get('input', None) else None
            except:
                inp = attrs.get('input', None)

            kwargs = {
                'date': event_dt,
                'activity_execution': ActivityExecution(attrs['activityType']['name'], attrs['activityId'], inp),
            }

            event_cls = ActivityEvent if result else ActivityStartedEvent
            if result:
                kwargs['result'] = result

            return (event_id, event_cls(**kwargs))

        if event_type == 'ActivityTaskStarted':
            return event_with_result(None)
        elif event_type == 'ActivityTaskCompleted':
            result = json.loads(attributes['result']) if 'result' in attributes.keys() else None
            return event_with_result(ActivityCompleted(result=result))
        elif event_type == 'ActivityTaskFailed':
            reason = attributes.get('reason', None)
            details = attributes.get('details', None)
            res = ActivityFailed(reason=reason, details=details)
            return event_with_result(res)
        elif event_type == 'ActivityTaskCanceled':
            details = attributes.get('details', None)
            return event_with_result(ActivityCanceled(details=details))
        elif event_type == 'ActivityTaskTimedOut':
            details = attributes.get('details', None)
            return event_with_result(ActivityTimedOut(details=details))

    @classmethod
    def _child_process_event(cls, event, related):
        event_id = event['eventId']
        event_dt = datetime.fromtimestamp(event['eventTimestamp'])
        event_type = event['eventType']
        attributes = cls._attributes(event)

        initiated_by = cls._related_event(related, attributes['initiatedEventId'])
        initiated_attr = initiated_by['startChildWorkflowExecutionInitiatedEventAttributes']

        pid = cls.pid_from_description(attributes['workflowExecution'])
        
        kwargs = {
            'datetime': event_dt,
            'workflow': initiated_attr['workflowType']['name'],
            'tags': initiated_attr['tagList']
        }
        
        if event_type == 'ChildWorkflowExecutionCompleted':
            json_res = json.loads(attributes['result']) if 'result' in attributes.keys() else None
            result = ProcessCompleted(result=json_res)
        elif event_type == 'ChildWorkflowExecutionCanceled':
            details = attributes.get('details', None)
            result = ProcessCanceled(details=details)
        elif event_type == 'ChildWorkflowExecutionTimedOut':
            result = ProcessTimedOut()
        else:
            return None

        return (event_id, ChildProcessEvent(pid, result, **kwargs))

    @classmethod
    def _signal_event(cls, event, related):
        event_id = event['eventId']
        event_dt = datetime.fromtimestamp(event['eventTimestamp'])
        event_type = event['eventType']
        attributes = cls._attributes(event)
        
        try:
            data = json.loads(attributes['input']) if 'input' in attributes.keys() else None
        except:
            data = attributes.get('input', None)
        name = attributes['signalName']
        
        return (event_id, SignalEvent(datetime=event_dt, signal=Signal(name=name, data=data)))

    @classmethod
    def _timer_event(cls, event, related):
        event_id = event['eventId']
        event_dt = datetime.fromtimestamp(event['eventTimestamp'])
        attributes = cls._attributes(event)

        started_by = filter(lambda x: x['eventId'] == attributes['startedEventId'], related)[0]
        started_attrs = started_by['timerStartedEventAttributes']
        timer = Timer(delay=int(started_attrs['startToFireTimeout']), data=json.loads(started_attrs['control']))
        return (event_id, TimerEvent(datetime=event_dt, timer=timer))

    @classmethod
    def event_from_description(cls, description, related=[]):
        event_type = description['eventType']
        if event_type == 'WorkflowExecutionStarted':
            dt = datetime.fromtimestamp(description['eventTimestamp'])
            return (description['eventId'], ProcessStartedEvent(datetime=dt))
        elif event_type == 'DecisionTaskStarted':
            dt = datetime.fromtimestamp(description['eventTimestamp'])
            return (description['eventId'], DecisionStartedEvent(datetime=dt))
        elif event_type in ['ActivityTaskScheduled','StartChildWorkflowExecutionInitiated','TimerStarted']:
            return cls._decision_event(description, related)
        elif event_type.startswith('ActivityTask') and not event_type == 'ActivityTaskScheduled':
            return cls._activity_event(description, related)
        elif event_type == 'WorkflowExecutionSignaled':
            return cls._signal_event(description, related)
        elif event_type.startswith('ChildWorkflowExecution'):
            return cls._child_process_event(description, related)
        elif event_type == 'TimerFired':
            return cls._timer_event(description, related)
        else:
            return None

    @classmethod
    def from_description(cls, description):
        execution_desc = description.get('workflowExecution', None) or description.get('execution', None)
        if not execution_desc:
            return None

        pid = cls.pid_from_description(execution_desc)

        workflow = description.get('workflowType', {}).get('name', None)
        tags = description.get('tagList', [])

        events = []
        event_descriptions = description.get('events', [])
        
        for event_description in event_descriptions:
            start_attrs = event_description.get('workflowExecutionStartedEventAttributes', None)
            if start_attrs:
                try:
                    input = json.loads(start_attrs['input'])
                except:
                    input = start_attrs.get('input', None)
                tags = start_attrs['tagList']
                
                parent = None
                parent_wfe = start_attrs.get('parentWorkflowExecution', None)
                if parent_wfe:
                    parent = cls.pid_from_description(parent_wfe)


            event = cls.event_from_description(event_description, related=event_descriptions)
            if event:
                events.append(event)

        # events is [(order, event)]. history is events sorted by logical order
        history = [event for (log_order, event) in sorted(events, key=lambda (log_order, event): int(log_order))]
        return AmazonSWFProcess(id=pid, workflow=workflow, input=input, tags=tags, history=history, parent=parent)

    @classmethod
    def pid_from_description(cls, description):
        return '%s:%s' % (description['workflowId'], description['runId'])
            
    