import logging
import threading
import datetime

_LOGGER = logging.getLogger(__name__)


class Service(object):
    def __init__(self, *args, **kwargs):
        self.__quit_ev = threading.Event()
        self.__start_lock = threading.Lock()
        self.__t = None

    def __schedule(self):
        """Figure out when we're next supposed to run, and schedule it."""

#        _LOGGER.debug("SCHEDULE: [%s]", self.__class__.__name__)

        try:
            wait_s = self.get_idle_interval_s()
        except NotImplementedError:
            wait_s = None
        else:
            assert wait_s > 0, "Wait interval must be positive: " + str(wait_s)

        try:
            wait_dt = self.get_next_run_dt()
        except NotImplementedError:
            wait_dt = None

        if ((wait_s is not None) ^ (wait_dt is not None)) is False:
            if wait_s is None:
                raise ValueError("Neither the interval nor the next-"
                                 "run time was set for the service.")
            else:
                raise ValueError("Only the interval *or* the next-"
                                 "run time should be set for the "
                                 "service.")

        if wait_dt is not None:
            now_dt = datetime.datetime.now()
            wait_s = (wait_dt - now_dt).seconds

            # This might occur due to resolution-related error if the time 
            # scheduled is minutely behind the current time.
            if wait_s < 0:
                _LOGGER.debug("Wakeup time is in the past: WHEN_S=(%d) "
                              "WHEN_DT=[%s] NOW_DT=[%s]", 
                              wait_s, wait_dt, now_dt)

                wait_s = 0

#        _LOGGER.debug("ABOUT TO CALL: [%s]", self.__class__.__name__)

        with self.__start_lock:
#            _LOGGER.debug("ABOUT TO CALL 2: [%s]", self.__class__.__name__)

            if self.__quit_ev.is_set() is True:
                return

#            _LOGGER.debug("TIMER [%s]: (%d)", self.__class__.__name__, wait_s)

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
            self.__cycle()
        # We were idle. Wait an interval before calling back.
        else:
            self.__schedule()

    def start(self):
        """Do startup tasks, here."""

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

    def get_idle_interval_s(self):
        """Return the number of seconds to wait when nothing is done. Mutually 
        exclusive with get_next_run_dt().
        """

        raise NotImplementedError()

    def get_next_run_dt(self):
        """Return the time at which the next cycle should be invoked. 
        Mutually exclusive with get_idle_interval_s().
        """

        raise NotImplementedError()
