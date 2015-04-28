import logging
import flask

import scheduler.views.blueprints.api

_LOGGER = logging.getLogger(__name__)

@scheduler.views.blueprints.api.API_BP.route('/job', methods=['GET'])
def api_job_get():

# TODO(dustin): Finish.

    response = {}

    raw_response = flask.jsonify(response)
    response = flask.make_response(raw_response)

    return (response, 200)
