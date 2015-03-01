import logging
import time

import scheduler.constants
import scheduler.config.reactor
import scheduler.registry
import scheduler.services.bus
import scheduler.services.jobs
import scheduler.services.queue

_LOGGER = logging.getLogger(__name__)


class Reactor(object):
    def __init__(self):
        self.__registry = scheduler.registry.get_registry()

        self.__bus_service = scheduler.services.bus.BusService()
        self.__registry.add_instance(
            scheduler.constants.SERVICE_BUS, 
            self.__bus_service)

        self.__jobs_service = scheduler.services.jobs.JobsService()
        self.__registry.add_instance(
            scheduler.constants.SERVICE_JOBS, 
            self.__jobs_service)

        self.__queue_service = scheduler.services.queue.QueueService()
        self.__registry.add_instance(
            scheduler.constants.SERVICE_QUEUE, 
            self.__queue_service)

    def run(self):
        started = []

        _LOGGER.info("Starting up.")

        self.__bus_service.start()
        started.append(self.__bus_service)

        try:
            self.__jobs_service.start()
            started.append(self.__jobs_service)

            self.__queue_service.start()
            started.append(self.__queue_service)

            # Enter main loop.

            _LOGGER.info("System ready.")

            while 1:
                time.sleep(scheduler.config.reactor.IDLE_WAIT_S)
        finally:
            _LOGGER.info("Shutting down.")

            # Shut the services down in reverse.
            for service_obj in started[::-1]:
                try:
                    service_obj.stop()
                except:
                    _LOGGER.exception("Could not stop service: [%s]", 
                                      service_obj.__class__.__name__)
