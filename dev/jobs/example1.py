import time
import datetime

def _call_em():
    with open('/tmp/called', 'w') as f:
        f.write(str(time.time()))

#RUN_AT_ABSOLUTE_OBJ = None
now_dt = datetime.datetime.now()
future_dt = (now_dt + datetime.timedelta(seconds=90))
RUN_AT_TIME_OBJ = future_dt.time()
#RUN_AT_INTERVAL_S = 30
RUN_ROUTINE = _call_em

RUN_SUCCESS_EMAIL_LIST = ['success@local']
RUN_FAIL_EMAIL_LIST = ['fail@local']
