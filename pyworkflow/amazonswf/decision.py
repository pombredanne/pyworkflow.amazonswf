import json
import uuid

from pyworkflow.decision import ScheduleActivity, CancelActivity, CompleteProcess, CancelProcess, StartChildProcess, Timer

class AmazonSWFDecision(object):
    def __init__(self, decision, config):
        if isinstance(decision, ScheduleActivity):
            description = self.schedule_activity_description(decision, config)
        elif isinstance(decision, CancelActivity):
            description = self.cancel_activity_description(decision)
        elif isinstance(decision, CompleteProcess):
            description = self.complete_process_description(decision)
        elif isinstance(decision, CancelProcess):
            description = self.cancel_process_description(decision)
        elif isinstance(decision, StartChildProcess):
            description = self.start_child_process_description(decision, config)
        elif isinstance(decision, Timer):
            description = self.timer_description(decision)
        else:
            raise Exception('Invalid decision type')

        self.description = description

    def schedule_activity_description(cls, decision, config):
        a_conf = config.for_activity(decision.activity, camelcase=True)
        attrs = {
            "activityId": str(decision.id),
            "activityType": {
              "name": decision.activity,
              "version": "1.0",
            },
            "control": None,
            "input": json.dumps(decision.input) if decision.input else None
        }

        attrs = dict(attrs.items() + a_conf.items())
        if decision.category:
            attrs['taskList'] = {
                "name": decision.category
            }

        return {
            "decisionType": "ScheduleActivityTask",
            "scheduleActivityTaskDecisionAttributes": attrs
        }

    def cancel_activity_description(self, decision):
        return {
            "decisionType": "RequestCancelActivityTask",
            "requestCancelActivityTaskDecisionAttributes": {
                "activityId": decision.id
            }
        }

    def complete_process_description(self, decision):
        return {
            "decisionType": "CompleteWorkflowExecution",
            "completeWorkflowExecutionDecisionAttributes": {
                "result": json.dumps(decision.result)
            }
        }

    def cancel_process_description(self, decision):
        return {
            "decisionType": "CancelWorkflowExecution",
            "cancelWorkflowExecutionDecisionAttributes": {
                "details": decision.details
            }
        }
        
    def start_child_process_description(self, decision, config):
        if decision.process.id is not None:
            raise ValueError('AmazonSWF does not support manually assigned ids on a process. Process.id should be None.')

        p_conf = config.for_workflow(decision.process.workflow, camelcase=True)
        attrs = {
            'workflowType': {
                'name': decision.process.workflow,
                'version': "1.0"
            },
            'workflowId': str(uuid.uuid4()),
            'childPolicy': decision.child_policy or 'ABANDON',
            'input': json.dumps(decision.process.input),
            'tagList': decision.process.tags
        }

        attrs = dict(attrs.items() + p_conf.items())

        return {
            "decisionType": "StartChildWorkflowExecution",
            "startChildWorkflowExecutionDecisionAttributes": attrs
        }

    def timer_description(self, decision):
        return {
            "decisionType": "StartTimer",
            "startTimerDecisionAttributes": {
                "timerId": str(uuid.uuid4()),
                "startToFireTimeout": str(decision.delay),
                "control": json.dumps(decision.data)
            }
        }