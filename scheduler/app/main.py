import logging
import flask
import threading

import scheduler.config.log
import scheduler.config.general
import scheduler.reactor

import scheduler.views.blueprints.api
import scheduler.views.importers.api

_LOGGER = logging.getLogger(__name__)

APP = flask.Flask(__name__)
APP.debug = scheduler.config.general.IS_DEBUG

APP.register_blueprint(scheduler.views.blueprints.api.API_BP)

# Start the scheduling subsystems.

def _scheduler_thread():
    _LOGGER.info("Scheduling running.")

    r = scheduler.reactor.Reactor()
    r.run()

def _start_scheduler():
    _LOGGER.debug("Starting scheduler.")

    t = threading.Thread(target=_scheduler_thread)
    t.start()

_start_scheduler()
