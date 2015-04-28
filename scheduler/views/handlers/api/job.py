import logging
import flask

import scheduler.constants
import scheduler.views.blueprints.api
import scheduler.registry

_LOGGER = logging.getLogger(__name__)

@scheduler.views.blueprints.api.API_BP.route('/job', methods=['GET'])
def api_job_get():

    r = scheduler.registry.get_registry()
    j = r.get_instance(scheduler.constants.SERVICE_JOBS)

    (jobs, state_hash) = j.get_last_reported_jobs()

    if jobs is None:
        jobs_info = {}
    else:
        jobs_info = dict([
                        (name, info[0].keys()) 
                        for (name, info) 
                        in jobs.items()])

    response = {
        'jobs': jobs_info,
        'state_hash': state_hash,
    }

    raw_response = flask.jsonify(response)
    response = flask.make_response(raw_response)

    return (response, 200)
