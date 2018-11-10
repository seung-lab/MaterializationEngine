__version__ = "0.0.2"


def create_app(test_config=None):
    from flask_sqlalchemy import SQLAlchemy
    from flask import Flask
    from .database import Base
    from materializationengine.config import configure_app
    from .admin import setup_admin
    from .materialize_bp import bp as materialize_bp
    from .utils import get_instance_folder_path
    from .database import db
    from .materialize_database import db_session

    # Define the Flask Object
    app = Flask(__name__,
                static_folder="../static",
                instance_path=get_instance_folder_path(),
                instance_relative_config=True)
    # load configuration (from test_config if passed)
    if test_config is None:
        app = configure_app(app)
    else:
        app.config.update(test_config)
    # register blueprints
    db.init_app(app)
    with app.app_context(): 
        db.create_all()
        admin = setup_admin(app, db)

    def shutdown_session(exception=None):
        db_session.remove()

    app.register_blueprint(materialize_bp)
    app.teardown_appcontext(shutdown_session)
    return app


