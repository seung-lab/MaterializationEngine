from materializationengine import create_app, create_celery
from celery import Celery

celery_app = Celery(include=[
    'materializationengine.workflows.ingest_new_annotations',
    'materializationengine.workflows.create_frozen_database',
    'materializationengine.workflows.update_root_ids',
    'materializationengine.workflows.bulk_upload',
    'materializationengine.workflows.complete_workflow',
    'materializationengine.workflows.dummy_workflow',
    'materializationengine.workflows.periodic_database_removal',
    'materializationengine.shared_tasks',
    ])

app = create_app()
celery = create_celery(app, celery_app)
