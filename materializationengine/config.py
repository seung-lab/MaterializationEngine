# Define the application directory
import os
from annotationengine.utils import get_app_base_path
import logging


class BaseConfig(object):
    HOME = os.path.expanduser("~")
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    # Statement for enabling the development environment
    DEBUG = True
    proj_dir = os.path.split(get_app_base_path())[0]

    INFOSERVICE_ENDPOINT = "http://35.196.170.230/info"
    BIGTABLE_CONFIG = {
        'instance_id': 'pychunkedgraph',
        'amdb_instance_id': 'pychunkedgraph',
        'project_id': "neuromancer-seung-import"
    }
    TESTING = False
    LOGGING_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
    LOGGING_LOCATION = HOME + '/.materializationengine/bookshelf.log'
    LOGGING_LEVEL = logging.DEBUG
    CHUNKGRAPH_TABLE_ID = "pinky40_fanout2_v7"
    MATERIALIZATION_POSTGRES_URI = "postgres:postgres@localhost:5432/postgres"


config = {
    "development": "annotationengine.config.BaseConfig",
    "testing": "annotationengine.config.BaseConfig",
    "default": "annotationengine.config.BaseConfig"
}


def configure_app(app):
    config_name = os.getenv('FLASK_CONFIGURATION', 'default')
    # object-based default configuration
    app.config.from_object(config[config_name])
    if 'MATERIALIZATION_ENGINE_SETTINGS' in os.environ.keys():
        app.config.from_envvar('MATERIALIZATION_ENGINE_SETTINGS')
    # instance-folders configuration
    app.config.from_pyfile('config.cfg', silent=True)

    return app
