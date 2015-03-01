import logging
import Queue
import time
import weakref
import threading

import scheduler.config.services.bus
import scheduler.services.service

_LOGGER = logging.getLogger(__name__)


class BusListener(object):
    def handle_message(self, name, data):
        raise NotImplementedError()


class BusService(scheduler.services.service.Service):
    def __init__(self, *args, **kwargs):
        super(BusService, self).__init__(*args, **kwargs)

        self.__listener_lock = threading.Lock()
# TODO(dustin): Come back to this. We're storing an object that is actively 
#               stored in the reactor, yet it's coming out of the weakref.
        self.__listeners = {}#weakref.WeakValueDictionary()
        self.__messages_q = Queue.Queue()

    def add_listener(self, name, handler_obj):
        assert issubclass(handler_obj.__class__, BusListener) is True, \
               "Bus-listener must be of type BusListener."

        with self.__listener_lock:
            _LOGGER.debug("Adding bus listener [%s].", name)

            if name in self.__listeners:
                raise ValueError("Bus listener [%s] is already registered." % 
                                 (name,))

            self.__listeners[name] = handler_obj

    def drop_listener(self, name):
        _LOGGER.debug("Dropping bus listener [%s].", name)
        del self.__listeners[name]

    def push_message(self, name, data=None):
        _LOGGER.debug("Pushing message: [%s]", name)
        self.__messages_q.put((name, data))

    def cycle(self):
#        _LOGGER.debug("BUS CYCLE")

        n = scheduler.config.services.bus.MAX_PROCESS_BATCH_SIZE
        while n > 0:
#            _LOGGER.debug("BUS CYCLE ITERATION")

            try:
                (name, data) = self.__messages_q.get(block=False)
            except Queue.Empty:
                return False

            try:
                handler = self.__listeners[name]
            except KeyError:
                _LOGGER.warning("Discarding unroutable bus message: [%s]", 
                                name)

                _LOGGER.debug("Listeners are:\n%s", dict(self.__listeners))
            else:
                _LOGGER.debug("Forwarding bus message of type [%s]: [%s]", 
                              name, handler.__class__.__name__)

                handler.handle_message(name, data)

            n -= 1

        return True

    def get_idle_interval_s(self):
        """Return the number of seconds to wait when nothing is done."""

        return scheduler.config.services.bus.IDLE_WAIT_S
