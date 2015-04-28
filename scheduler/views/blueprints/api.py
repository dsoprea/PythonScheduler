import logging
import flask

_LOGGER = logging.getLogger(__name__)

API_BP = flask.Blueprint('api', __name__, url_prefix='/api')
