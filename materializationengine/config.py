# Define the application directory
import os
import logging
from materializationengine.models import Base
from flask_sqlalchemy import SQLAlchemy


class BaseConfig(object):
    HOME = os.path.expanduser("~")
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    # Statement for enabling the development environment
    DEBUG = False

    INFOSERVICE_ENDPOINT = "http://info-service/info"
    BIGTABLE_CONFIG = {
        'instance_id': 'pychunkedgraph',
        'amdb_instance_id': 'pychunkedgraph',
        'project_id': "neuromancer-seung-import"
    }
    TESTING = False
    LOGGING_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
    LOGGING_LOCATION = HOME + '/.materializationengine/bookshelf.log'
    LOGGING_LEVEL = logging.DEBUG
    CHUNKGRAPH_TABLE_ID = "pinky100_sv16"
    SQLALCHEMY_DATABASE_URI = "postgres://postgres:synapsedb@localhost:5432/testing"
    DATABASE_CONNECT_OPTIONS = {}
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = "MYSUPERSECRETTESTINGKEY"

config = {
    "development": "materializationengine.config.BaseConfig",
    "testing": "materializationengine.config.BaseConfig",
    "default": "materializationengine.config.BaseConfig"
}


def configure_app(app):
    config_name = os.getenv('FLASK_CONFIGURATION', 'default')
    # object-based default configuration
    app.config.from_object(config[config_name])
    if 'MATERIALIZATION_ENGINE_SETTINGS' in os.environ.keys():
        app.config.from_envvar('MATERIALIZATION_ENGINE_SETTINGS')
    # instance-folders configuration
    app.config.from_pyfile('config.cfg', silent=True)
    print(app.config)
    db = SQLAlchemy(model_class=Base)
    db.init_app(app)


    return app
