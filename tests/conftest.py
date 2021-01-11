from celery.app.base import app_has_custom
import pytest
from materializationengine import create_app, create_celery
from materializationengine.celery_worker import celery_app

import datetime

ALIGNED_VOLUME = 'test_aligned_volume'
DATASTACK = 'test_datastack'
TEST_TIMESTAMP = datetime.datetime.utcnow()
SCHEMA = 'synapse'
MAT_VERSION = 1
ANNOTATION_TABLE_NAME = 'test_synapse_table'

@pytest.fixture(scope='session')
def app():
    yield create_app()

@pytest.fixture(scope='session')
def celery_worker(app):
    celery = create_celery(app, celery_app)
    yield celery

@pytest.fixture(scope='session')
def celery_config():
    return {
        'broker_url': 'redis://',
        'result_backend': 'redis://'
    }