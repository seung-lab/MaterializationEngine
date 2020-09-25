from materializationengine import create_app, create_celery
from celery import Celery

celery = Celery(include=[
    'materializationengine.workflows.live_materialization',
    'materializationengine.workflows.flat_materialization',
    'materializationengine.shared_tasks',

    ])

app = create_app()
celery = create_celery(app, celery)
