import logging
import datetime
import Queue
import threading
import collections

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
                            'is_running', 
                            'is_empty', 
                            'next_run_s', 
                            'next_run_dt'
                        ])


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
        # We set us to this state initially because we still ned to be told to 
        # update. We don't have any data.
        self.__state = None
        self.__set_updating_state()

    def __get_now_dt(self):
        now_dt = datetime.datetime.now()
        now_dt = now_dt.replace(microsecond=0)

        return now_dt

    def __get_next_run_dt_from_time(self, time_obj, start_from_dt=None):
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

        if start_from_dt is None:
            start_from_dt = datetime.datetime.now()

        time_obj = time_obj.replace(microsecond=0)

        start_from_time_obj = start_from_dt.time()

        if start_from_time_obj < time_obj:
            next_run_dt = scheduler.utility.update_time(start_from_dt, time_obj)
        else:
            tomorrow_dt = start_from_dt + datetime.timedelta(seconds=86400)
            next_run_dt = scheduler.utility.update_time(tomorrow_dt, time_obj)

        return next_run_dt

    def __get_next_run_dt_from_interval(self, interval_s):
        now_dt = self.__get_now_dt()
        next_run_dt = now_dt + datetime.timedelta(seconds=interval_s)

        return next_run_dt

    def __set_updating_state(self):
        _LOGGER.debug("Setting task queue to UPDATING state (and then going "
                      "to sleep).")

        self.__state = _SCHEDULER_STATE_CLS(
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
                        is_running=True, 
                        is_empty=False,
                        next_run_s=None, 
                        next_run_dt=next_run_dt)

    def __set_idle_state(self):
        _LOGGER.debug("Setting task queue to IDLE state (and then going to "
                      "sleep).")

        self.__state = _SCHEDULER_STATE_CLS(
                        is_running=True, 
                        is_empty=True,
                        next_run_s=\
                            scheduler.config.services.queue.\
                                NO_JOB_WAKEUP_INTERVAL_S, 
                        next_run_dt=None)

    def __get_absolute_dt_from_definition(self, definition, start_from_dt=None):
        elected_types = [name 
                         for name 
                         in scheduler.config.services.jobs.TIME_FIELDS 
                         if definition.get(name) is not None]

        elected_types_len = len(elected_types)

        if elected_types_len > 1:
            raise ValueError("Job [%s] elects too many different timing-"
                             "types." % (name,))
        elif elected_types_len == 0:
            raise ValueError("Job [%s] elects no timing-types." % (name,))

        timing_type = elected_types[0]

# TODO(dustin): Still need to test absolute times.
        if timing_type == scheduler.constants.JF_TIME_ABSOLUTE_OBJ:
            absolute_dt = definition[scheduler.constants.JF_TIME_ABSOLUTE_OBJ]
# TODO(dustin): Still need to test time objects.
        elif timing_type == scheduler.constants.JF_TIME_TIME_OBJ:
            absolute_dt = self.__get_next_run_dt_from_time(
                            definition[scheduler.constants.JF_TIME_TIME_OBJ],
                            start_from_dt=start_from_dt)
        elif timing_type == scheduler.constants.JF_TIME_INTERVAL_S:
            interval_s = definition[scheduler.constants.JF_TIME_INTERVAL_S]

            assert interval_s > 0, \
                   "Interval must be greater than zero seconds."

            absolute_dt = self.__get_next_run_dt_from_interval(
                            interval_s)
        else:
            raise ValueError("Unexpected fallthrough: [%s]" % 
                             (timing_type,))

        # Round off the time to the second so that all times align to the 
        # second.
        absolute_dt = absolute_dt.replace(microsecond=0)

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
            (when_dt, _) = item

            self.__schedule_q.put(item)
            self.__set_ready_state(when_dt)

    def __update_schedule(self, jobs_dict):
        _LOGGER.debug("Updating task schedule.")

        with self.__schedule_q_lock:
            self.__set_updating_state()

            self.__jobs_dict = jobs_dict

            run_times = []
            now_dt = self.__get_now_dt()
            for name, code_info in jobs_dict.items():
                (definition, g, l) = code_info

                try:
                    next_dt = self.__get_absolute_dt_from_definition(
                                    definition)
                except:
                    _LOGGER.exception("Error while deriving wakeup time from "
                                      "definition for [%s].", name)
                    raise

                if next_dt < now_dt:
                    _LOGGER.warning("Job [%s] will never be called: [%s]. Not "
                                    "scheduling.", name, next_dt)

                    continue

                run_times.append((next_dt, (name, definition, g, l)))

            # Load scheduling queue.

            self.__schedule_q = Queue.PriorityQueue()
            for when, context in run_times:
                self.__schedule_q.put((when, context))

            # Schedule our next wakeup, and set our state.
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
#            self.__schedule_q.put((scheduled_dt, context))
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

        # Pop any adjacent jobs that have a matching time.

        while 1:
            try:
                (following_dt, context) = self.__schedule_q.get(block=False)
            except Queue.Empty:
                break

            assert following_dt >= scheduled_dt, \
                   "The next jobs appeared to be scheduled for earlier than " \
                   "the last."

            if following_dt != scheduled_dt:
                self.__schedule_q.put((following_dt, context))
                break

            jobs.append(context)

#        _LOGGER.debug("Retrieved (%d) jobs from the queue to process.", 
#                      len(jobs))

        return (scheduled_dt, jobs)

    def cycle(self):
        """Run the next scheduled task. 

        Note that, since we're managing all of our own timing by hot-wiring the 
        service-layer into waking-us up pursuant to our next scheduled-job, we 
        will always return False. This, essentially, tells the service-layer 
        that we didn't do anything and to wait for the configured amount of 
        time (which is dynamically adjusted).
        """

        if self.__state.is_running is False:
            _LOGGER.debug("Task queue is currently updating. We're in a "
                          "holding state.")
            return False

        with self.__schedule_q_lock:
            _LOGGER.debug("Running cycle.")

            result = self.__get_next_jobs()

            if issubclass(result.__class__, bool) is True:
                return result

            (scheduled_dt, jobs) = result

            now_dt = self.__get_now_dt()
            
            # In practice, we're woken up a tiny-bit too early, so this is 
            # usually negative.
            delay_s = (now_dt - scheduled_dt).total_seconds()

            _LOGGER.info("Dequeued (%d) jobs. NOW_DT=[%s] EXPECTED_DT=[%s] "
                         "SCHEDULE_DELAY_S=(%.2f)", 
                         len(jobs), now_dt, scheduled_dt, delay_s)

            # Now, reschedule these jobs for their next invocation (we don't 
            # wait until they're done executing).

            for context in jobs:
                (name, definition, g, l) = context

                # Reque immediately so that a) we can release the lock, and b) we 
                # can allow ourselves to forgive exceptions without interrpt future 
                # attempts.

                next_dt = self.__get_absolute_dt_from_definition(
                            definition, 
                            start_from_dt=scheduled_dt)

                if next_dt < now_dt:
                    _LOGGER.warning("Job [%s] will never [again] be called: "
                                    "[%s]. Not rescheduling.", name, next_dt)

                    continue

                _LOGGER.debug("Proactively rescheduling job [%s] for [%s].", 
                              name, next_dt)

                self.__schedule_q.put((next_dt, context))
            
            self.__peek_and_schedule()

# TODO(dustin): We should fork this into its own thread after we've tested.

# TODO(dustin): We usually pull the first task to be run off the queue almost 
#               immediately, instead of honoring its schedule. Fix this.
        for context in jobs:
            self.__run_task(context)

        return False

    def __run_task(self, context):
        (name, definition, g, l) = context

        _LOGGER.debug("Running job: [%s]", name)

        fn = definition['RUN_ROUTINE']
        
        try:
            exec fn.__code__ in l
        except:
            self.__bus.push_message(
                scheduler.constants.MT_QUEUE_TASK_FAILED,
                (name, traceback.format_exc()))
        else:
            self.__bus.push_message(
                scheduler.constants.MT_QUEUE_TASK_SUCCESS,
                name)

    def get_invocation_delay(self):
        if not ((self.__state.next_run_s is None) ^ (self.__state.next_run_dt is None)):
            raise ValueError("Next-run seconds and date-time are mutually-"
                             "exclusive.")

        if self.__state.next_run_s is not None:
            if self.__state.is_empty is True:
                _LOGGER.debug("Task queue is empty. Sleeping.")
            else:
                _LOGGER.debug("It looks like the job queue is currently "
                              "updating. Sleeping.")

            return scheduler.services.service.InvocationDelay(
                    self.__state.next_run_s)
        else:
            _LOGGER.debug("Scheduler going to sleep until: [%s]", 
                          self.__state.next_run_dt)

            return scheduler.services.service.InvocationDateTimeDelay(
                    self.__state.next_run_dt)
