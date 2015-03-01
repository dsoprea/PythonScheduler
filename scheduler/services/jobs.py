import logging
import threading
import time
import os
import hashlib

import scheduler.constants
import scheduler.config.services.jobs
import scheduler.registry
import scheduler.services.service
import scheduler.services.bus

_LOGGER = logging.getLogger(__name__)

_CONFIG_PREFIX = 'RUN_'


class JobsService(scheduler.services.service.Service):
    """Manages the jobs that are available to run."""

    def __init__(self, *args, **kwargs):
        super(JobsService,self).__init__(*args, **kwargs)

        self.__registry = scheduler.registry.get_registry()
        self.__bus = self.__registry.get_instance(
                        scheduler.constants.SERVICE_BUS)

        self.__last_state_hash = None
        self.__jobs = None

    def __update_jobs(self):
        path = scheduler.config.services.jobs.JOB_PATH
# Use inotify once we finish that project.
        job_filenames = os.listdir(path)

        # Determine if anything has changed.

        state_list = []
        for filename in job_filenames:
            filepath = os.path.join(path, filename)
            epoch = os.stat(filepath).st_mtime
            state_list.append(filename + '=' + str(epoch))

        state_hash = hashlib.md5(','.join(state_list)).hexdigest()

        if state_hash == self.__last_state_hash:
            return False

        # Reload the jobs.

        _LOGGER.info("Reloading jobs.")

        jobs = {}
        for filename in job_filenames:
            pivot = filename.index('.')
            name = filename[:pivot]
            filepath = os.path.join(path, filename)

            # Compile the config as Python code.

            with open(filepath) as f:
                obj = compile(f.read(), '(JOB: ' + filename + ')', 'exec')

            # Now, "execute" the code, and pull out the locals (the variables 
            # that are defined).

            g = {
                '__builtins__': __builtins__, 
                '__name__': 'job:' + name, 
                '__doc__': None, 
                '__package__': None,
            }

            l = {}

            try:
                exec obj in g, l
            except:
                _LOGGER.exception("There was an uncaught exception while reading: [%s]", name)

            definition = dict([(k, v) 
                               for (k, v) 
                               in l.items() 
                               if k.startswith(_CONFIG_PREFIX)])

            jobs[name] = (definition, g, l)

        self.__jobs = jobs
        self.__last_state_hash = state_hash

        # Notify the scheduler that there has been a change.

        self.__bus.push_message(
            scheduler.constants.MT_QUEUE_POKE, 
            self.__jobs)

        return True

    def cycle(self):
        """The main body of logic for a single cycle of the service occurs 
        here.
        """

        return self.__update_jobs()

    def get_idle_interval_s(self):
        """Return the number of seconds to wait when nothing is done."""

        return scheduler.config.services.jobs.UPDATE_CHECK_INTERVAL_S

    def start(self):
        self.__update_jobs()
        super(JobsService, self).start()
