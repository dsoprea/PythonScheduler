import os

import scheduler.constants

JOB_PATH = os.environ['SCHED_JOB_PATH']

UPDATE_CHECK_INTERVAL_S = 10

TIME_FIELDS = [
    scheduler.constants.JF_TIME_ABSOLUTE_OBJ, 
    scheduler.constants.JF_TIME_TIME_OBJ,
    scheduler.constants.JF_TIME_INTERVAL_S,
]

REQUIRED_FIELDS = [
    scheduler.constants.JF_ROUTINE,
]

ALL_FIELDS = [
    scheduler.constants.JF_TIME_ABSOLUTE_OBJ, 
    scheduler.constants.JF_TIME_TIME_OBJ,
    scheduler.constants.JF_TIME_INTERVAL_S,
    scheduler.constants.JF_ROUTINE,
    scheduler.constants.JF_EMAIL_SUCCESS_EMAIL_LIST,
    scheduler.constants.JF_EMAIL_FAIL_EMAIL_LIST,
]

ALERT_SUBJECT_JOB_READ_FAILED = "Job definition has errors: {{ job_name }}"
ALERT_MESSAGE_JOB_READ_FAILED = "The job definition had errors."

ALERT_SUBJECT_QUEUE_TASK_SUCCESS = "Job was successful: {{ job_name }}"
ALERT_MESSAGE_QUEUE_TASK_SUCCESS = "The job was successful."

ALERT_SUBJECT_QUEUE_TASK_FAILED = "Job failed: {{ job_name }}"
ALERT_MESSAGE_QUEUE_TASK_FAILED = "The job failed."
