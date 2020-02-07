from app.factories.flask_app import create_app
from app.factories.celery_app import create_celery
from app.extensions import celery

app = create_app()
celery = create_celery(app, celery)
