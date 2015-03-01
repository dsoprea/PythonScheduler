import time

def _call_em():
    with open('/tmp/called', 'w') as f:
        f.write(str(time.time()))

RUN_AT_ABSOLUTE_OBJ = None
RUN_AT_TIME_OBJ = None
RUN_AT_INTERVAL_S = 30
RUN_ROUTINE = _call_em
