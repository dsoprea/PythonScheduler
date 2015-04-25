import scheduler.constants

# This should equal our error (scheduler.config.general.TIMING_PRECISION_S). 
# Any less, and we risk detecting false jitter and going back to sleep rapidly 
# and for long sequences.
UPDATE_WAKEUP_INTERVAL_S = 5

NO_JOB_WAKEUP_INTERVAL_S = 60

#TIMING_TYPES_S = set([
#    scheduler.constants.JF_TIME_ABSOLUTE_OBJ,
#    scheduler.constants.JF_TIME_TIME_OBJ,
#    scheduler.constants.JF_TIME_INTERVAL_S
#])
