from materializationengine.app import create_app
from materializationengine.celery_app import create_celery
from materializationengine.celery_init import celery
from celery.schedules import crontab
from materializationengine.workflows.periodic_database_removal import remove_expired_databases
from materializationengine.workflows.periodic_materialization import run_periodic_materialzation
from materializationengine.schemas import CeleryBeatSchema
from materializationengine.errors import TaskNotFound
from celery.app import builtins
from celery.utils.log import get_task_logger

app = create_app()
celery = create_celery(app, celery)

celery_logger = get_task_logger(__name__)



@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):

    periodic_tasks = {
        'run_daily_periodic_materialzation': run_periodic_materialzation.s(days_to_expire=2),
        'run_weekly_periodic_materialzation': run_periodic_materialzation.s(days_to_expire=7),
        'run_lts_periodic_materialzation': run_periodic_materialzation.s(days_to_expire=30),
        'remove_expired_databases': remove_expired_databases.s(delete_threshold=5),
    }

    # remove expired task results in redis broker
    sender.add_periodic_task(crontab(hour=0, minute=0, day_of_week='*', day_of_month='*', month_of_year='*'),
                             builtins.add_backend_cleanup_task(celery), name="Clean up back end results")

    beat_schedules = app.config['BEAT_SCHEDULES']
    celery_logger.info(beat_schedules)
    schedules = CeleryBeatSchema(many=True).dump(beat_schedules)
    for schedule in schedules:

        if schedule['task'] in periodic_tasks:
            task = periodic_tasks[schedule['task']]
            sender.add_periodic_task(crontab(minute=schedule['minute'],
                                             hour=schedule['hour'],
                                             day_of_week=schedule['day_of_week'],
                                             day_of_month=schedule['day_of_month'],
                                             month_of_year=schedule['month_of_year']),
                                     task,
                                     name=schedule['name'])
        else:
            raise TaskNotFound(schedule['task'], periodic_tasks)
