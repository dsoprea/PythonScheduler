import logging
import datetime
import Queue
import threading
import collections
import traceback

import scheduler.constants
import scheduler.config.services.queue
import scheduler.config.services.jobs
import scheduler.registry
import scheduler.services.service
import scheduler.services.bus
import scheduler.utility

_LOGGER = logging.getLogger(__name__)

_SCHEDULER_STATE_CLS = collections.namedtuple(
                        '_SCHEDULER_STATE_CLS',
                        [
                            'name',
                            'is_running', 
                            'is_empty', 
                            'next_run_s', 
                            'next_run_dt'
                        ])

_JOB_CONTEXT = collections.namedtuple(
                '_JOB_CONTEXT', [
                    'name', 
                    'definition',
                    'locals',
                ])

_DAY_LENGTH_S = 86400

# States.

_S_UPDATING = 'updating'
_S_READY = 'ready'
_S_IDLE = 'idle'


class QueueService(
        scheduler.services.service.Service,
        scheduler.services.bus.BusListener):
    def __init__(self, *args, **kwargs):
        super(QueueService, self).__init__(*args, **kwargs)

        self.__registry = scheduler.registry.get_registry()
        self.__bus = self.__registry.get_instance(
                        scheduler.constants.SERVICE_BUS)

        self.__schedule_q = Queue.PriorityQueue()
        self.__schedule_q_lock = threading.Lock()

        self.__jobs_dict = {}

        self.__bus.add_listener(scheduler.constants.MT_QUEUE_POKE, self)

        # We'll toggle between waking-up at a particular time, and waking up 
        # "in a little bit". We want to wake-up at a certain time. However, when
        # we're updating we'll be configuring to delay one second at a time.
        #
        # We set us to this state initially because we still need to be told to 
        # update. We don't have any data.
        self.__state = None
        self.__set_updating_state()

    def __normalize_time_obj(self, obj):
        """Take a time or datetime object and set the seconds to (0), for 
        alignment.
        """

        return obj.replace(microsecond=0)

    def __get_now_dt(self):
        now_dt = datetime.datetime.now()
        return self.__normalize_time_obj(now_dt)

    def __get_next_run_dt_from_time(self, time_obj, start_from_dt):
        """Given a time-of-day, figure-out the datetime of the next occurence 
        (largely, either that time today or that time tomorrow).

        We're use this to determine the next time to run a task. It might be a
        common assumption to calculate the next time to run based on the last 
        time the task was run, and to assume that the current time is equal-to 
        or greater than the time that the current task was scheduled to run.
        This is not correct, however: The threading package's timer module 
        triggers our tasks slighty earlier than scheduled. This means that, 
        when we do the comparison to determine if the task's time is in the 
        past or the future, we'll schedule to run the task a moment later.

        Therefore, start_from_dt can be given to base our timings on, to 
        circumvent this undesirable behavior.
        """

        time_obj = self.__normalize_time_obj(time_obj)

        start_from_time_obj = start_from_dt.time()

        if start_from_time_obj < time_obj:
            _LOGGER.debug("Scheduling for LATER today (time-based "
                          "scheduling): [%s] < [%s]", 
                          start_from_time_obj, time_obj)

            next_run_dt = \
                scheduler.utility.update_time(start_from_dt, time_obj)
        else:
            _LOGGER.debug("Scheduling for TOMORROW (time-based "
                          "scheduling): [%s] >= [%s]", 
                          start_from_time_obj, time_obj)

            tomorrow_dt = \
                start_from_dt + datetime.timedelta(seconds=_DAY_LENGTH_S)

            next_run_dt = scheduler.utility.update_time(tomorrow_dt, time_obj)

        _LOGGER.debug("Job time was an absolute TIME: [%s] => [%s] "
                      "START-FROM=[%s]", 
                      time_obj, next_run_dt, start_from_time_obj)

        return next_run_dt

    def __get_next_run_dt_from_interval(self, interval_s):
        now_dt = self.__get_now_dt()
        next_run_dt = now_dt + datetime.timedelta(seconds=interval_s)

        _LOGGER.debug("Interval of (%d) seconds translated to absolute: [%s] "
                      "[%s]", interval_s, now_dt, next_run_dt)

        return next_run_dt

    def __set_updating_state(self):
        _LOGGER.debug("Setting task queue to UPDATING state (and then going "
                      "to sleep).")

        self.__state = _SCHEDULER_STATE_CLS(
                        name=_S_UPDATING,
                        is_running=False, 
                        is_empty=False,
                        next_run_s=\
                            scheduler.config.services.queue.\
                                UPDATE_WAKEUP_INTERVAL_S,
                        next_run_dt=None)

    def __set_ready_state(self, next_run_dt):
        _LOGGER.debug("Setting task queue to READY state (and then going to "
                      "sleep).")

        now_dt = self.__get_now_dt()

        assert next_run_dt >= now_dt, \
               "Next-run time is in past: [%s]" % (next_run_dt,)

        self.__state = _SCHEDULER_STATE_CLS(
                        name=_S_READY,
                        is_running=True, 
                        is_empty=False,
                        next_run_s=None, 
                        next_run_dt=next_run_dt)

    def __set_idle_state(self):
        _LOGGER.debug("Setting task queue to IDLE state (and then going to "
                      "sleep).")

        self.__state = _SCHEDULER_STATE_CLS(
                        name=_S_IDLE,
                        is_running=True, 
                        is_empty=True,
                        next_run_s=\
                            scheduler.config.services.queue.\
                                NO_JOB_WAKEUP_INTERVAL_S, 
                        next_run_dt=None)

    def __get_absolute_dt_from_context(self, context):
        timing_info = self.__validate_job_context(context)
        if timing_info is None:
            _LOGGER.warning("Not scheduling job (2): [%s]", context.name)
            return None

        (timing_type, timing_obj) = timing_info

        if timing_type == scheduler.constants.JF_TIME_ABSOLUTE_OBJ:
            absolute_dt = timing_obj
        elif timing_type == scheduler.constants.JF_TIME_TIME_OBJ:
            now_dt = self.__get_now_dt()
            start_from_with_error_dt = \
                now_dt + scheduler.config.general.TIMING_PRECISION_TD

            absolute_dt = self.__get_next_run_dt_from_time(
                            timing_obj,
                            start_from_with_error_dt)
        elif timing_type == scheduler.constants.JF_TIME_INTERVAL_S:
            absolute_dt = self.__get_next_run_dt_from_interval(timing_obj)
        else:
            raise ValueError("Unexpected fallthrough: [%s]" % 
                             (timing_type,))

        absolute_dt = self.__normalize_time_obj(absolute_dt)

        return absolute_dt

    def __peek_and_schedule(self):
        """Read the first item from the queue, re-add it, and update our next-
        run time. Since this is a priority-queue and the first value will 
        always be the lowest, we'll always be reading the next timestamp that 
        we should be run, we'll always be putting it back at the head of the 
        queue, and it will always be a cheap operation.
        """

        try:
            item = self.__schedule_q.get(block=False)
        except Queue.Empty:
            self.__set_idle_state()
        else:
            (when_dt, data) = item

            self.__add_to_schedule(when_dt, data, check_proximity=False)
            self.__set_ready_state(when_dt)

    def __update_schedule(self, jobs_dict):
        _LOGGER.debug("Updating task schedule.")

        with self.__schedule_q_lock:
            self.__set_updating_state()

            self.__jobs_dict = jobs_dict

            run_times = []
            now_dt = self.__get_now_dt()
            for name, code_info in jobs_dict.items():
                (definition, l) = code_info

                context = _JOB_CONTEXT(
                            name=name, 
                            definition=definition, 
                            locals=l)

                self.__validate_job_context(context)

                try:
                    next_dt = self.__get_absolute_dt_from_context(context)
                except:
                    _LOGGER.exception("Error while deriving wakeup time from "
                                      "definition for [%s].", context.name)
                    raise

                if next_dt is None:
                    _LOGGER.warning("Job will never be run: [%s]", 
                                    context.name)

                    continue

                run_times.append((next_dt, context))

            # Load scheduling queue.

            self.__schedule_q = Queue.PriorityQueue()
            for when, context in run_times:
                self.__add_to_schedule(when, context)

            # Schedule our next wakeup, and set our state.
            
            _LOGGER.debug("Schedule has been updated. Setting wakeup state "
                          "for next job.")

            self.__peek_and_schedule()

    def handle_message(self, name, data):
        if name == scheduler.constants.MT_QUEUE_POKE:
            # We've received a poke with a dictionary of all of the jobs. 
            # Update the schedule.

            self.__update_schedule(data)
        else:
            raise ValueError("Could not handle bus-message of type: [%s]" % \
                             (name,))

    def __get_next_jobs(self):
        """Pop the next job, as well as any adjacent job that is scheduled for 
        the same time. Since we wakeup and run based on the time of the next 
        task to run, the first job we pop will generally have a time that 
        matches the current time.
        """

        # Pop the first.

        try:
            (scheduled_dt, context) = self.__schedule_q.get(block=False)
        except Queue.Empty:
            # Consistency check.
            assert self.__state.is_empty is True, \
                   "The schedule was empty, but it's not supposed to be."

            return False

        now_dt = self.__get_now_dt()

#        # If we're receiving new jobs or recalculating the schedule, we'll be 
#        # in a holding pattern and switch to a schedule where we wake-up every 
#        # couple of seconds. When we transition between this state and when 
#        # we're sleeping between jobs, we may lose some precision. So, we trap 
#        # that anomaly here.
#        if scheduled_dt > now_dt:
#            # Add the task back to the schedule at the original datetime.
##            self.__schedule_q.put((scheduled_dt, context))
#            self.__add_to_schedule(scheduled_dt, context)
#
#            _LOGGER.warning("We ran too early (this may be normal). Going "
#                            "back to sleep until [%s].", scheduled_dt)
#
#            # By returning an empty-list of jobs, we won't do anything, and 
#            # we'll automatically determine when we need to wake-up.
#            return (scheduled_dt, [])

        # Consistency check.
        assert self.__state.is_empty is False, \
               "The schedule was supposed to be empty."

        jobs = [context]

        # Pop any adjacent jobs that have a matching time. By "matching time",
        # We consider the time that was scheduled as well as anything that was 
        # scheduled within the range of error just afterward.

        cutoff_dt = \
            (scheduled_dt + scheduler.config.general.TIMING_PRECISION_TD)

        while 1:
            try:
                (following_dt, context) = self.__schedule_q.get(block=False)
            except Queue.Empty:
                break

            assert following_dt >= scheduled_dt, \
                   "The next jobs appeared to be scheduled for earlier than " \
                   "the last."

            if following_dt > cutoff_dt:
                # The next job is too long after the others to do at this time.
                # Note that this means that if we have a bunch of tasks that 
                # were scheduled in the distant past, we'll work through them 
                # in clusters, either ignoring them or reschedulign them in the 
                # future as relevant.

                self.__add_to_schedule(following_dt, context)
                break

            jobs.append(context)

        return (scheduled_dt, jobs)

    def cycle(self):
        """Run the next scheduled task. 

        Note that, since we're managing all of our own timing by hot-wiring the 
        service-layer into waking-us up pursuant to our next scheduled-job, we 
        will always return False. This, essentially, tells the service-layer 
        that we didn't do anything and to wait for the configured amount of 
        time (which is dynamically adjusted).
        """

        if self.__state.name != _S_READY:
            _LOGGER.debug("Task queue is currently not running (we're "
                          "updating or have no jobs).")

            return False

        # If a change to when we were to be called was made out-of-band, then 
        # the time that we were already scheduled to wake-up at will likely be 
        # invalid. If we were scheduled for a particular time and it's still in 
        # the future, return False to force ourselves back to sleep with the 
        # current wake-up time. Note that this requires us to take the position 
        # that we'll only ever process a job that was scheduled in the past 
        # (though usually only a brief moment into the past).
        if self.__state.next_run_dt is not None:
            now_dt = self.__get_now_dt()
            
            if self.__state.next_run_dt > now_dt:
                _LOGGER.debug("We woke earlier then expected, probably "
                              "because of a state-change. Going back to "
                              "sleep. SCHED=[%s] NOW=[%s]", 
                              self.__state.next_run_dt, now_dt)

                return False
            else:
                _LOGGER.debug("We were scheduled for a particular time, and it "
                              "looks like we're on time (or it was supposed to be "
                              "run in the past). No need to sleep to compensate. "
                              "SCHED=[%s] NOW=[%s]", 
                              self.__state.next_run_dt, now_dt)

        # At this point, we'll expect to find a job that is supposed to be run 
        # or one or more jobs that are old and need to either be discarded or 
        # rescheduled to run at a more appropriate time. The latter will often 
        # happen at start-up.

        with self.__schedule_q_lock:
            _LOGGER.debug("Running cycle.")

            result = self.__get_next_jobs()

            if issubclass(result.__class__, bool) is True:
# TODO(dustin): Our assumption here was that we woke up early. This probably 
#               isn't valid. Whatever the case, we should take a step back and 
#               recalculate the delay before going back to sleep.

# TODO(dustin): This may turn out to be unnecessary. This is just a precautionary measure.
                _LOGGER.warning("When we tried to pull jobs, we found none. "
                                "Going back to sleep.")

                return result

            (scheduled_dt, jobs) = result
            
            now_dt = self.__get_now_dt()

            error_td = (now_dt - scheduled_dt)
            error_s = error_td.total_seconds()
            _LOGGER.debug("Received (%d) jobs for a scheduled-time of [%s]. "
                          "NOW_DT=[%s] ERROR_S=(%d)", 
                          len(jobs), scheduled_dt, now_dt, error_s)

#            # In practice, we're woken up a tiny-bit too early, so this is 
#            # usually negative.
#            delay_s = (now_dt - scheduled_dt).total_seconds()
#
#            _LOGGER.info("Dequeued (%d) jobs. NOW_DT=[%s] EXPECTED_DT=[%s] "
#                         "SCHEDULE_DELAY_S=(%.2f)", 
#                         len(jobs), now_dt, scheduled_dt, delay_s)

            # Now, reschedule these jobs for their next invocation (we don't 
            # wait until they're done executing).

            for context in jobs:
                # Reque immediately so that a) we can release the lock, and b) we 
                # can allow ourselves to forgive exceptions without interrpt future 
                # attempts.

                next_dt = self.__get_absolute_dt_from_context(context)
                if next_dt < now_dt:
                    _LOGGER.warning("Job [%s] will never [again] be called: "
                                    "[%s]. Not rescheduling.", 
                                    context.name, next_dt)

                    continue

                _LOGGER.debug("Proactively rescheduling job [%s] for [%s].", 
                              context.name, next_dt)

                self.__reschedule_job_for_next(next_dt, context)
            
            self.__peek_and_schedule()

# TODO(dustin): We should fork this into its own thread after we've tested.

        # If we had an time-based or absolute datetime-based jobs, they 
        # would've alredy been scheduled for their next invocation (or 
        # ignored). However, we don't want to actually run them, here.
        #
        # Note that we not only skip the first job, but also the other ones 
        # that we grabbed with it. These were determined to be scheduled close 
        # enough that they were in the range of timing error, and shouldn't be 
        # scheduled again.
        if error_s < 0 and \
           abs(error_td) > scheduler.config.general.TIMING_PRECISION_TD:
            names = [c.name for c in jobs]

            _LOGGER.warning("Not running the actual jobs because they're too "
                            "old and probably needed to be rescheduled for "
                            "the future: %s", names)
        else:
            map(self.__run_task, jobs)

        return False

    def __reschedule_job_for_next(self, last_run_dt, context):
        timing_info = self.__validate_job_context(context)
        if timing_info is None:
            _LOGGER.warning("Not scheduling job: [%s]", name)
            return

        (timing_type, timing_obj) = timing_info

        now_dt = self.__get_now_dt()

        if timing_type == scheduler.constants.JF_TIME_ABSOLUTE_OBJ:
            absolute_dt = timing_obj
        elif timing_type == scheduler.constants.JF_TIME_TIME_OBJ:
            start_from_with_error_dt = \
                now_dt + scheduler.config.general.TIMING_PRECISION_TD

            absolute_dt = self.__get_next_run_dt_from_time(
                            timing_obj,
                            start_from_with_error_dt)
        elif timing_type == scheduler.constants.JF_TIME_INTERVAL_S:
            absolute_dt = self.__get_next_run_dt_from_interval(timing_obj)
        else:
            raise ValueError("Unexpected fallthrough (2): [%s]" % 
                             (timing_type,))

        # An job with an absolute time will, by definition, only be run once.
        if timing_type == scheduler.constants.JF_TIME_ABSOLUTE_OBJ:
            _LOGGER.info("Job with absolute-time will not be rescheduled: "
                         "[%s] [%s]", context.name, absolute_dt)
        else:
            self.__add_to_schedule(absolute_dt, context)

    def __validate_job_context(self, context):
        _LOGGER.debug("Validating context: [%s]", context.name)

        elected_types = [attr_name 
                         for attr_name 
                         in scheduler.config.services.jobs.TIME_FIELDS 
                         if context.definition.get(attr_name) is not None]

        elected_types_len = len(elected_types)

        if elected_types_len > 1:
            raise ValueError("Job [%s] elects too many different timing-"
                             "types." % (elected_types,))
        elif elected_types_len == 0:
            raise ValueError("Job [%s] elects no timing-types." % 
                             (elected_types,))

        timing_type = elected_types[0]

        now_dt = self.__get_now_dt()

        if timing_type == scheduler.constants.JF_TIME_ABSOLUTE_OBJ:
            absolute_dt = \
                context.definition[scheduler.constants.JF_TIME_ABSOLUTE_OBJ]

            if absolute_dt < now_dt:
                _LOGGER.warning("Task [%s] is scheduled for an absolute time "
                                "in the past and will be ignored: ABS [%s] < "
                                "NOW [%s]", context.name, absolute_dt, now_dt)
                return None
            else:
                return (timing_type, absolute_dt)

        elif timing_type == scheduler.constants.JF_TIME_TIME_OBJ:
            time_obj = context.definition[scheduler.constants.JF_TIME_TIME_OBJ]

            return (
                timing_type, 
                time_obj)
        elif timing_type == scheduler.constants.JF_TIME_INTERVAL_S:
            interval_s = \
                context.definition[scheduler.constants.JF_TIME_INTERVAL_S]

            assert interval_s > 0, \
                   "Interval must be greater than zero seconds."

            return (timing_type, interval_s)
        else:
            raise ValueError("Timing-type of job is not valid: [%s]" % 
                             (timing_type,))

    def __add_to_schedule(self, timestamp_dt, data, check_proximity=True):
        if check_proximity is True:
            now_dt = self.__get_now_dt()

            now_with_error_dt = \
                now_dt + \
                scheduler.config.general.TIMING_PRECISION_TD

            if now_dt < timestamp_dt < now_with_error_dt:
                raise ValueError("The time that we're scheduling for is too "
                                 "near. It falls within our range of error. "
                                 "SCHEDULE-FOR=[%s] NOW=[%s] "
                                 "NOW-WITH-ERROR=[%s]" % 
                                 (timestamp_dt, now_dt, now_with_error_dt))

        self.__schedule_q.put((timestamp_dt, data))

    def __run_task(self, context):
        _LOGGER.debug("Running job: [%s]", context.name)

        fn = context.definition['RUN_ROUTINE']
        
        try:
            exec fn.__code__ in context.locals
        except:
            _LOGGER.exception("Task failed: [%s]", context.name)

            self.__bus.push_message(
                scheduler.constants.MT_QUEUE_TASK_FAILED,
                (context.name, traceback.format_exc()))
        else:
            self.__bus.push_message(
                scheduler.constants.MT_QUEUE_TASK_SUCCESS,
                context.name)

    def get_invocation_delay(self):
        if not ((self.__state.next_run_s is None) ^ (self.__state.next_run_dt is None)):
            raise ValueError("Next-run seconds and date-time are mutually-"
                             "exclusive.")

        if self.__state.next_run_s is not None:
            if self.__state.is_empty is True:
                _LOGGER.debug("Task queue is empty. Sleeping.")
            else:
                _LOGGER.debug("It looks like the job queue is currently "
                              "updating. Sleeping for (%d) seconds.",
                              self.__state.next_run_s)

            return scheduler.services.service.InvocationDelay(
                    self.__state.next_run_s)
        else:
            _LOGGER.debug("Scheduler going to sleep until: [%s]", 
                          self.__state.next_run_dt)

            return scheduler.services.service.InvocationDateTimeDelay(
                    self.__state.next_run_dt)
