# Define the application directory
import os
import logging
from emannotationschemas.models import Base
from flask_sqlalchemy import SQLAlchemy



class BaseConfig:
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
    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "sqlite://")    #"postgres://postgres:synapsedb@db:5432/synapsedb"
    DATABASE_CONNECT_OPTIONS = {}
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = "MYSUPERSECRETTESTINGKEY"
    REDIS_HOST = "redis"
    REDIS_PORT = 6379
    CELERY_BROKER_URL = os.environ.get(
        'REDIS_URL', "redis://{host}:{port}/0".format(
        host=REDIS_HOST, port=str(REDIS_PORT)))
    CELERY_RESULT_BACKEND = CELERY_BROKER_URL


class DevConfig(BaseConfig):
    DEBUG = True


class TestConfig(BaseConfig):
    TESTING = True


class ProductionConfig(BaseConfig):
    LOGGING_LEVEL = logging.INFO
    CELERY_BROKER = os.environ.get('REDIS_URL')
    CELERY_RESULT_BACKEND = os.environ.get('REDIS_URL')
    REDIS_URL = os.environ.get('REDIS_URL')


config = {
    "default": "app.config.BaseConfig",
    "development": "app.config.DevConfig",
    "testing": "app.config.TestConfig",
    "production": "app.config.ProductionConfig",
}


def configure_app(app):
    config_name = os.getenv('FLASK_CONFIGURATION', 'default')
    # object-based default configuration
    app.config.from_object(config[config_name])
    if 'MATERIALIZATION_ENGINE_SETTINGS' in os.environ.keys():
        app.config.from_envvar('MATERIALIZATION_ENGINE_SETTINGS')
    # instance-folders configuration
    app.config.from_pyfile('config.cfg', silent=True)
    app.logger.debug(app.config)
    db = SQLAlchemy(model_class=Base)
    db.init_app(app)
    app.app_context().push()
    return app