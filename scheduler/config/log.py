import logging
import logging.handlers
import os

import scheduler.config.general

_FMT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
_FORMATTER = logging.Formatter(_FMT)

def _add_stream_handler(logger):
    ch = logging.StreamHandler()
    ch.setFormatter(_FORMATTER)
    ch.setLevel(logging.DEBUG)

    logger.addHandler(ch)

def _add_syslog_handler(logger):
    ch = logging.handlers.SysLogHandler()
    ch.setFormatter(_FORMATTER)

    # This doesn't seem to hit (syslog never shows debugging info). It might be 
    # syslog filtering them out.
    if scheduler.config.general.IS_DEBUG is True:
        ch.setLevel(logging.DEBUG)
    else:
        ch.setLevel(logging.INFO)

    logger.addHandler(ch)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

_add_syslog_handler(logger)

_DO_DEBUG_SCREEN_LOG = bool(int(os.environ.get('SCHED_DO_DEBUG_SCREEN_LOG', '1')))
if scheduler.config.general.IS_DEBUG is True and _DO_DEBUG_SCREEN_LOG is True:
    _add_stream_handler(logger)
