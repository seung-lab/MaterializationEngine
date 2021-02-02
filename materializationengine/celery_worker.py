from materializationengine.app import create_app
from materializationengine.celery_app import create_celery
from materializationengine.celery_init import celery
from celery.schedules import crontab
from materializationengine.workflows.periodic_database_removal import remove_expired_databases
from materializationengine.workflows.periodic_materialization import run_periodic_materialzation
from materializationengine.workflows.dummy_workflow import dummy_arg_task

app = create_app()
celery = create_celery(app, celery)


@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):

    # Materialize database every Monday and Friday, expires in 7 days from creation"
    sender.add_periodic_task(crontab(hour=0, minute=10, day_of_week=[
                             1, 5]), run_periodic_materialzation.s(7), name="Materialized Database (7 Days)")

    # Materialize a "Long Term Support” database every 1st and 3rd Wednesday, expires in 30 days from creation"
    sender.add_periodic_task(crontab(hour=0, minute=10, day_of_week=3, day_of_month='1-7,15-21'),
                             run_periodic_materialzation.s(30), name='Long Term Support Materialized Database (30 days)')
    # Remove (drop) expired databases every night at midnight"
    sender.add_periodic_task(crontab(hour=0, minute=0), remove_expired_databases.s(5), name="Remove Expired Databases (Midnight)")
