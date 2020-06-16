from materializationengine import create_app, create_celery
from materializationengine.extensions import celery

app = create_app()
celery = create_celery(app, celery)
