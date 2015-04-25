import time
import datetime

def _call_em():
    with open('/tmp/called', 'w') as f:
        f.write(str(time.time()))

_THIRTY_SECONDS_TD = datetime.timedelta(seconds=30)
_NOW_DT = datetime.datetime.now()

RUN_AT_ABSOLUTE_OBJ = _NOW_DT + _THIRTY_SECONDS_TD

#future_dt = (_NOW_DT + _THIRTY_SECONDS_TD)
#RUN_AT_TIME_OBJ = future_dt.time()

#RUN_AT_TIME_OBJ = datetime.time(hour=15, minute=39, second=33)

#RUN_AT_INTERVAL_S = 30

RUN_ROUTINE = _call_em

RUN_SUCCESS_EMAIL_LIST = ['success@local']
RUN_FAIL_EMAIL_LIST = ['fail@local']
