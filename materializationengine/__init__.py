from .materialize_bp import bp as materialize_bp
from .utils import get_instance_folder_path

__version__ = "0.0.1"


def create_app(test_config=None):
    from flask import Flask
    from annotationengine.config import configure_app

    # Define the Flask Object
    app = Flask(__name__,
                instance_path=get_instance_folder_path(),
                instance_relative_config=True)
    # load configuration (from test_config if passed)
    if test_config is None:
        app = configure_app(app)
    else:
        app.config.update(test_config)
    # register blueprints
    app.register_blueprint(materialize_bp)

    with app.app_context():
       pass
    return app
