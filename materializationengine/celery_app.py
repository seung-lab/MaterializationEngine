from celery.signals import after_setup_logger
import logging
import sys
from celery.schedules import crontab


def create_celery(app=None, celery=None):

    celery.conf.result_backend = app.config['CELERY_RESULT_BACKEND']
    celery.conf.broker_url = app.config['CELERY_BROKER_URL']
    celery.conf.update({'task_routes': ('materializationengine.task_router.TaskRouter'),
                        'task_serializer': 'pickle',
                        'result_serializer': 'pickle',
                        'accept_content': ['pickle'],
                        'optimization':'fair',
                        'worker_prefetch_multiplier': 1})
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


