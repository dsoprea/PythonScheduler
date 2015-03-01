import logging
import threading
import weakref

_LOGGER = logging.getLogger(__name__)


class _Registry(object):
    def __init__(self):
        self.__instances_lock = threading.Lock()
        self.__instances = weakref.WeakValueDictionary()

    def add_instance(self, name, obj):
        with self.__instances_lock:
            if name in self.__instances:
                raise ValueError("An instance has already been registered "
                                 "with that name: [%s]", name)

            self.__instances[name] = obj

    def drop_instance(self, name):
        del self.__instances[name]

    def get_instance(self, name):
        return self.__instances[name]

_INSTANCE = None
def get_registry():
    global _INSTANCE

    if _INSTANCE is None:
        _INSTANCE = _Registry()

    return _INSTANCE
