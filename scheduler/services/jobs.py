import logging
import threading
import time
import os
import hashlib
import traceback

import scheduler.constants
import scheduler.config.services.jobs
import scheduler.registry
import scheduler.services.service
import scheduler.services.bus
import scheduler.email_template

_LOGGER = logging.getLogger(__name__)

_CONFIG_PREFIX = 'RUN_'


class JobsService(
        scheduler.services.service.Service,
        scheduler.services.bus.BusListener):
    """Manages the jobs that are available to run."""

    def __init__(self, *args, **kwargs):
        super(JobsService, self).__init__(*args, **kwargs)

        self.__registry = scheduler.registry.get_registry()
        self.__bus = self.__registry.get_instance(
                        scheduler.constants.SERVICE_BUS)

        self.__bus.add_listener(
            scheduler.constants.MT_QUEUE_TASK_SUCCESS, 
            self)
        
        self.__bus.add_listener(
            scheduler.constants.MT_QUEUE_TASK_FAILED, 
            self)

        self.__last_state_hash = None
        self.__jobs = None

    def __send_job_message(self, job_name, is_success, subject, message, 
                           data=None):
        if is_success is True:
            email_list = scheduler.config.general.ADMIN_SUCCESS_EMAIL_LIST[:]
        else:
            email_list = scheduler.config.general.ADMIN_FAIL_EMAIL_LIST[:]

        # We might not have loaded the jobs, yet.
        if self.__jobs is not None:
            context = self.__jobs[job_name]
            (definition, g, l) = context

            if is_success is True:
                email_list_field = scheduler.constants.JF_EMAIL_SUCCESS_EMAIL_LIST
            else:
                email_list_field = scheduler.constants.JF_EMAIL_FAIL_EMAIL_LIST

            email_list_job = definition.get(email_list_field, [])
            email_list += email_list_job

        if not email_list:
            _LOGGER.debug("No email will be sent because no email addresses "
                          "were configured: JOB=[%s] IS_SUCCESS=[%s]", 
                          job_name, is_success)

            return

        email = scheduler.email_template.EmailTemplate(
                    email_list,
                    subject,
                    text_body_template=message)

        replacements = {
            'job_name': job_name,
        }

        email.send(replacements)

    def handle_message(self, message_name, data):
        if message_name == scheduler.constants.MT_QUEUE_TASK_SUCCESS:
            job_name = data

            self.__send_job_message(
                job_name, 
                True,
                scheduler.config.services.jobs.ALERT_SUBJECT_QUEUE_TASK_SUCCESS,
                scheduler.config.services.jobs.ALERT_MESSAGE_QUEUE_TASK_SUCCESS,
                None)
        elif message_name == scheduler.constants.MT_QUEUE_TASK_FAILED:
            (job_name, traceback_text) = data

            self.__send_job_message(
                job_name, 
                False,
                scheduler.config.services.jobs.ALERT_SUBJECT_QUEUE_TASK_FAILED,
                scheduler.config.services.jobs.ALERT_MESSAGE_QUEUE_TASK_FAILED,
                traceback_text)
        else:
            raise ValueError("Did not handle message: [%s]" % (name,))

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
        all_fields_s = set(scheduler.config.services.jobs.ALL_FIELDS)
        required_fields_s = set(scheduler.config.services.jobs.REQUIRED_FIELDS)
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
                _LOGGER.exception("There was an uncaught exception while "
                                  "reading: [%s]", name)

                self.__send_job_message(
                    name, 
                    scheduler.config.services.jobs.ALERT_SUBJECT_JOB_READ_FAILED,
                    scheduler.config.services.jobs.ALERT_MESSAGE_JOB_READ_FAILED,
                    traceback.format_exc())

            definition = dict([(k, v) 
                               for (k, v) 
                               in l.items() 
                               if k.startswith(_CONFIG_PREFIX)])

            present_fields_s = set(definition.keys())

            try:
                if present_fields_s.issuperset(required_fields_s) is False:
                    raise ValueError("Required fields are not [all] present: %s" % 
                                     (required_s,))
                elif all_fields_s.issuperset(present_fields_s) is False:
                    raise ValueError("One or more fields are not valid: %s" % 
                                     (present_fields_s - all_fields_s,))
            except:
                _LOGGER.exception("Definition is not valid: [%s]", name)

                self.__send_job_message(
                    name, 
                    scheduler.config.services.jobs.ALERT_SUBJECT_JOB_READ_FAILED,
                    scheduler.config.services.jobs.ALERT_MESSAGE_JOB_READ_FAILED,
                    traceback.format_exc())
            else:
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
