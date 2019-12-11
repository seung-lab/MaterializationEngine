__version__ = "0.1.2"


def create_app(test_config=None):
    from flask_sqlalchemy import SQLAlchemy
    from flask import Flask
    from celery import Celery
    from app.config import configure_app
    from app.admin import setup_admin
    from app.api import bp as materialize_bp
    from .utils import get_instance_folder_path
    from app.database import db, Base
    import logging
    
    # Define the Flask Object
    app = Flask(__name__,
                static_folder="../static",
                instance_path=get_instance_folder_path(),
                instance_relative_config=True)
    # load configuration (from test_config if passed)
    logging.basicConfig(level=logging.DEBUG)

    if test_config is None:
        app = configure_app(app)
    else:
        app.config.update(test_config)
    # register blueprints
    db.init_app(app)
    with app.app_context(): 
        db.create_all()
        admin = setup_admin(app, db)

    app.register_blueprint(materialize_bp)

    celery = Celery(app.name, broker=app.config['CELERY_BROKER'])
    celery.conf.update(app.config)
    return app
    
if __name__ == "__main__":
    create_app.run(debug=True, host="0.0.0.0", port=80)