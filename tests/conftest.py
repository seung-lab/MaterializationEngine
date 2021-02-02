import datetime
import os

import pytest
from materializationengine.app import create_app
from materializationengine.celery_app import create_celery
from materializationengine.celery_init import celery as celery_instance

TEMP_ENV_VARS = {
    'SQLALCHEMY_DATABASE_URI': "sqlite://",
    'REDIS_URL': "redis://",
    'CELERY_BROKER_URL': 'redis://',
    'CELERY_RESULT_BACKEND': 'redis://',
}


@pytest.fixture(scope='session', autouse=True)
def test_env_setup_and_teardown():
    old_environ = dict(os.environ)
    os.environ.update(TEMP_ENV_VARS)
    yield
    os.environ.clear()
    os.environ.update(old_environ)


@pytest.fixture(scope='module')
def mat_metadata():
    timestamp = datetime.datetime.utcnow()
    return {
        'aligned_volume': 'test_aligned_volume',
        'datastack': 'test_datastack',
        'schema': 'synapse',
        'timestamp': timestamp,
        'mat_version': 1,
        'annotation_table_name': 'test_synapse_table',
        'expires_timestamp': timestamp + datetime.timedelta(days=5),
        'version': 1, }


@pytest.fixture(scope='module')
def app():
    flask_app = create_app(test_config=True)
    # Create a test client using the Flask application configured for testing
    with flask_app.test_client() as testing_client:
        # Establish an application context
        with flask_app.app_context():
            yield testing_client  #


@pytest.fixture(scope='module')
def celery_app(app):
    celery = create_celery(app, celery_instance)
    yield celery
