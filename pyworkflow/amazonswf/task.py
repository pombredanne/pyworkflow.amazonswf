import json

from pyworkflow.activity import ActivityExecution
from pyworkflow.task import DecisionTask, ActivityTask

from process import AmazonSWFProcess

def decision_task_from_description(description):
    token = description.get('taskToken', None)
    if not token:
        return None

    process = AmazonSWFProcess.from_description(description)
    return DecisionTask(process, context={'token': token})

def activity_task_from_description(description):
    token = description.get('taskToken', None)
    if not token:
        return None

    activity_id = description['activityId']
    activity = description['activityType']['name']
    input = json.loads(description.get('input')) if description.get('input', None) else None
    activity_execution = ActivityExecution(activity=activity, id=activity_id, input=input)

    pid = AmazonSWFProcess.pid_from_description(description['workflowExecution'])

    return ActivityTask(activity_execution=activity_execution, process_id=pid, context={'token': token})