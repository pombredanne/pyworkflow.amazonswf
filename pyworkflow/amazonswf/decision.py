import json
import uuid

from pyworkflow.decision import ScheduleActivity, CancelActivity, CompleteProcess, CancelProcess, StartChildProcess, Timer

class AmazonSWFDecision(object):
    def __init__(self, decision):
        if isinstance(decision, ScheduleActivity):
            description = self.schedule_activity_description(decision)
        elif isinstance(decision, CancelActivity):
            description = self.cancel_activity_description(decision)
        elif isinstance(decision, CompleteProcess):
            description = self.complete_process_description(decision)
        elif isinstance(decision, CancelProcess):
            description = self.cancel_process_description(decision)
        elif isinstance(decision, StartChildProcess):
            description = self.start_child_process_description(decision)
        elif isinstance(decision, Timer):
            description = self.timer_description(decision)
        else:
            raise Exception('Invalid decision type')

        self.description = description

    def schedule_activity_description(cls, decision):
        return {
            "decisionType": "ScheduleActivityTask",
            "scheduleActivityTaskDecisionAttributes": {
                "activityId": str(decision.id),
                "activityType": {
                  "name": decision.activity,
                  "version": "1.0",
                },
                "control": None,
                "input": json.dumps(decision.input) if decision.input else None,
                "taskList": {
                    "name": decision.category or "default"
                }
            }
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
        
    def start_child_process_description(self, decision):
        if decision.process.id is not None:
            raise ValueError('AmazonSWF does not support manually assigned ids on a process. Process.id should be None.')

        return {
            "decisionType": "StartChildWorkflowExecution",
            "startChildWorkflowExecutionDecisionAttributes": {
                'workflowType': {
                    'name': decision.process.workflow,
                    'version': "1.0"
                },
                'workflowId': str(uuid.uuid4()),
                'childPolicy': decision.child_policy or 'ABANDON',
                'input': json.dumps(decision.process.input),
                'tagList': decision.process.tags
            }
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