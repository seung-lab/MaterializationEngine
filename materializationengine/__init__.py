from flask_sqlalchemy import SQLAlchemy
from flask import Flask, Blueprint, jsonify, redirect
from materializationengine.config import configure_app
from materializationengine.admin import setup_admin
from materializationengine.views import views_bp
from materializationengine.utils import get_instance_folder_path
from materializationengine.schemas import ma
from materializationengine.blueprints.materialize.api import mat_bp
from materializationengine.blueprints.client.api import client_bp
from materializationengine.models import Base

from celery.signals import after_setup_logger
from flask_restx import Api
import logging
import sys

__version__ = "0.2.35"

db = SQLAlchemy(model_class=Base)


def create_app(test_config=None):
    # Define the Flask Object
    app = Flask(__name__,
                static_folder="../static",
                instance_path=get_instance_folder_path(),
                instance_relative_config=True,
                template_folder="../templates")
    # load configuration (from test_config if passed)
    logging.basicConfig(level=logging.INFO)

    if test_config is None:
        app = configure_app(app)
    else:
        app.config.update(test_config)
    # register blueprints

    apibp = Blueprint('api', __name__, url_prefix='/materialize/api')

    db.init_app(app)
    ma.init_app(app)
    with app.app_context():
        api = Api(apibp, title="Materialization Engine API", version=__version__, doc="/doc")
        api.add_namespace(mat_bp, path='/v2')
        api.add_namespace(client_bp, path='/v2')

        app.register_blueprint(apibp)
        app.register_blueprint(views_bp)

        db.init_app(app)
        db.create_all()
        admin = setup_admin(app, db)
   
    @app.route("/health")
    def health():
        return jsonify("healthy"), 200

    @app.route("/materialize/")
    def index():
        return redirect("/materialize/views")

    return app


def create_celery(app=None, celery=None):

    celery.conf.result_backend = app.config['CELERY_RESULT_BACKEND']
    celery.conf.broker_url = app.config['CELERY_BROKER_URL']
    celery.conf.update({'task_routes': ('materializationengine.task_router.TaskRouter'),
                        'task_serializer': 'pickle',
                        'result_serializer': 'pickle',
                        'accept_content': ['pickle']})
    celery.conf.update(app.config)
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery


@after_setup_logger.connect
def celery_loggers(logger, *args, **kwargs):
    """
    Display the Celery banner appears in the log output.
    https://www.distributedpython.com/2018/10/01/celery-docker-startup/
    """
    logger.info(f'Customize Celery logger, default handler: {logger.handlers[0]}')
    logger.addHandler(logging.StreamHandler(sys.stdout))