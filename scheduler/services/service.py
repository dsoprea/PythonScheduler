import logging
import threading
import datetime
import math

_MAX_ERROR_CHECK_CYCLES = 100
_MAX_ERROR_CHECK_HISTORY_SIZE = 10

_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.INFO)


class InvocationDelay(object):
    def __init__(self, seconds):
        self.__seconds = seconds

    def get_seconds(self):
        return self.__seconds


class InvocationDateTimeDelay(InvocationDelay):
    def __init__(self, dt):
        now_dt = datetime.datetime.now()
        seconds = (dt - now_dt).total_seconds()
        _LOGGER.debug("InvocationDateTimeDelay: Wake-up at [%s] translates to "
                      "(%d) seconds.", dt, seconds)

        assert seconds > 0, \
               "Time-delay translates to zero or negative seconds: " \
               "[%s] - [%s] = (%d)s" % (dt, now_dt, seconds)

        super(InvocationDateTimeDelay, self).__init__(seconds)


class Service(object):
    def __init__(self, *args, **kwargs):
        self.__quit_ev = threading.Event()
        self.__start_lock = threading.Lock()
        self.__t = None

    def do_run_immediately(self):
        """Whether to run before scheduling, or schedule before running the 
        first time.
        """

        return True

    def __schedule(self):
        """Figure out when we're next supposed to run, and schedule it."""

#        _LOGGER.debug("SCHEDULE: [%s]", self.__class__.__name__)

        delay = self.get_invocation_delay()
        assert issubclass(delay.__class__, InvocationDelay), \
               "Invocation-delay must be an InvocationDelay."

        wait_s = delay.get_seconds()

        _LOGGER.debug("[%s] will wakeup in (%d) seconds.", 
                      self.__class__.__name__, wait_s)

        with self.__start_lock:
            if self.__quit_ev.is_set() is True:
                return

# TODO(dustin): Do we have to clean this up?

            self.__t = threading.Timer(wait_s, self.__cycle)
            self.__t.start()

    def __cycle(self):
        """Call the main service logic."""

        if self.__quit_ev.is_set() is True:
            return

        result = self.cycle()

        assert issubclass(result.__class__, bool), \
               "Cycle return-value must be boolean."

        # We found something to do. Call back, immediately.
        if result is True:
            _LOGGER.debug("Running service-cycle again, immediately.")
            self.__cycle()
        # We were idle. Wait an interval before calling back.
        else:
            _LOGGER.debug("Scheduling service for sleep.")
            self.__schedule()

    def start(self):
        """Do startup tasks, here."""

        if self.do_run_immediately() is True:
            self.__cycle()
        else:
            self.__schedule()

    def stop(self):
        """Do shutdown tasks, here."""

        with self.__start_lock:
            # We set this just in case the thread is already running. On the 
            # one hand, this means that we'll be very efficient in how we 
            # trigger but that a proper stop will block on the task to 
            # finish, but, on the other hand, we'll likely never be properly 
            # killed, anyway (e.g. via a signal, instead).
            self.__quit_ev.set()

            self.__t.cancel()
            self.__t.join()

    def cycle(self):
        """The main body of logic for a single cycle of the service occurs 
        here.
        """

        raise NotImplementedError()

    def get_invocation_delay(self):
        """Return an InvocationDelay instance describing when to invoke the 
        next cycle.
        """

        raise NotImplementedError()
