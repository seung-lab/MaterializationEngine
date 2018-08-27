from flask import Flask

from . import config

# from pychunkedgraph.app import app_blueprint
from .app_blueprint import bp as me_bp
import logging


def create_app(test_config=None):
    app = Flask(__name__)

    configure_app(app)

    if test_config is not None:
        app.config.update(test_config)

    app.register_blueprint(me_bp)
    # app.register_blueprint(app_blueprint.bp)

    return app


def configure_app(app):

    # Load logging scheme from config.py
    app.config.from_object(config.BaseConfig)

    # Configure logging
    handler = logging.FileHandler(app.config['LOGGING_LOCATION'])
    handler.setLevel(app.config['LOGGING_LEVEL'])
    formatter = logging.Formatter(app.config['LOGGING_FORMAT'])
    handler.setFormatter(formatter)
    app.logger.addHandler(handler)


