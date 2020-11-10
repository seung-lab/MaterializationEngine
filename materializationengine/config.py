# Define the application directory
import os
import logging
from emannotationschemas.models import Base
from flask_sqlalchemy import SQLAlchemy
import json


class BaseConfig:
    HOME = os.path.expanduser("~")
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    # Statement for enabling the development environment
    BIGTABLE_CONFIG = {
        "instance_id": "pychunkedgraph",
        "amdb_instance_id": "pychunkedgraph",
        "project_id": "neuromancer-seung-import",
    }
    TESTING = False
    LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    LOGGING_LOCATION = HOME + "/.materializationengine/bookshelf.log"
    LOGGING_LEVEL = logging.DEBUG
    CHUNKGRAPH_TABLE_ID = "pinky100_sv16"
    MATERIALIZATION_POSTGRES_URI = "postgres://postgres:materialize@db:5432/materialize"
    SQLALCHEMY_DATABASE_URI = "postgres://postgres:materialize@db:5432/materialize"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    REDIS_HOST = os.environ.get("REDIS_SERVICE_HOST")
    REDIS_PORT = os.environ.get("REDIS_SERVICE_PORT")
    REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
    CELERY_BROKER_URL = REDIS_URL
    CELERY_RESULT_BACKEND = REDIS_URL
    ANNO_ENDPOINT = "https://minniev1.microns-daf.com/annotation/"
    INFOSERVICE_ENDPOINT = "https://global.daf-apis.com/info"
    AUTH_URI = "https://global.daf-apis.com/auth"
    GLOBAL_SERVER = "https://global.daf-apis.com"
    SCHEMA_SERVICE_ENDPOINT = "https://global.daf-apis.com/schema/"
    SEGMENTATION_ENDPOINT = "https://global.daf-apis.com/"
    MATERIALIZATION_ROW_CHUNK_SIZE = 500
    QUERY_LIMIT_SIZE = 200000
    if os.environ.get("DAF_CREDENTIALS", None) is not None:
        with open(os.environ.get("DAF_CREDENTIALS"), "r") as f:
            AUTH_TOKEN = json.load(f)["token"]
    else:
        AUTH_TOKEN = ""

class DevConfig(BaseConfig):
    DEBUG = True


class TestConfig(BaseConfig):
    TESTING = True


class ProductionConfig(BaseConfig):
    LOGGING_LEVEL = logging.INFO
    CELERY_BROKER = os.environ.get("REDIS_URL")
    CELERY_RESULT_BACKEND = os.environ.get("REDIS_URL")
    REDIS_URL = os.environ.get("REDIS_URL")


config = {
    "default": "materializationengine.config.BaseConfig",
    "development": "materializationengine.config.DevConfig",
    "testing": "materializationengine.config.TestConfig",
    "production": "materializationengine.config.ProductionConfig",
}


def configure_app(app):
    config_name = os.getenv("FLASK_CONFIGURATION", "default")
    # object-based default configuration
    app.config.from_object(config[config_name])
    if "MATERIALIZATION_ENGINE_SETTINGS" in os.environ.keys():
        app.config.from_envvar("MATERIALIZATION_ENGINE_SETTINGS")
    # instance-folders configuration
    app.config.from_pyfile("config.cfg", silent=True)
    app.logger.debug(app.config)
    db = SQLAlchemy(model_class=Base)
    db.init_app(app)
    app.app_context().push()
    return app
