import os
import datetime

IS_DEBUG = bool(int(os.environ.get('IS_DEBUG', '0')))

_DEFAULT_ADMIN_EMAIL_LIST_RAW = os.environ.get('SCHED_EMAIL_ADMIN_LIST', '')

_ADMIN_SUCCESS_EMAIL_LIST_RAW = os.environ.get(
                                    'SCHED_EMAIL_ADMIN_LIST_SUCCESS', 
                                    _DEFAULT_ADMIN_EMAIL_LIST_RAW).strip()

if _ADMIN_SUCCESS_EMAIL_LIST_RAW != '':
    ADMIN_SUCCESS_EMAIL_LIST = _ADMIN_SUCCESS_EMAIL_LIST_RAW.split(',')
else:
    ADMIN_SUCCESS_EMAIL_LIST = []

_ADMIN_FAIL_EMAIL_LIST_RAW = os.environ.get(
                                'SCHED_EMAIL_ADMIN_LIST_FAIL', 
                                _DEFAULT_ADMIN_EMAIL_LIST_RAW).strip()

if _ADMIN_FAIL_EMAIL_LIST_RAW != '':
    ADMIN_FAIL_EMAIL_LIST = _ADMIN_FAIL_EMAIL_LIST_RAW.split(',')
else:
    ADMIN_FAIL_EMAIL_LIST = []

EMAIL_FROM_NAME = 'Scheduler'

# The amount of error that we will expect or account for in when we wake-up 
# compared with when we were *scheduled* to wake-up.
TIMING_PRECISION_S = 5
TIMING_PRECISION_TD = datetime.timedelta(seconds=TIMING_PRECISION_S)
