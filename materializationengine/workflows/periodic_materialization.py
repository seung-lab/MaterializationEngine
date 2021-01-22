"""
Create frozen dataset.
"""
import json
import os
from typing import List

from celery.utils.log import get_task_logger
from flask import current_app
from materializationengine.blueprints.materialize.api import get_datastack_info
from materializationengine.workflows.complete_workflow import \
    run_complete_worflow

celery_logger = get_task_logger(__name__)


SQL_URI_CONFIG = current_app.config["SQLALCHEMY_DATABASE_URI"]


def _get_datastacks() -> List:
    raise NotImplementedError


def run_periodic_materialzation() -> None:
    """
    Run complete materialization workflow. Steps are as follows:
    1. Find missing segmentation data in a given datastack and lookup.
    2. Update expired root ids
    3. Copy database to new frozen version
    4. Merge annotation and segmentation tables together
    5. Drop non-materializied tables
    """
    datastacks = json.loads(os.environ['DATASTACKS'])
    expires_in_n_days = os.environ['EXPIRES_IN_N_DAYS']

    for datastack in datastacks:
        try:
            print(f"Start periodic materialziation job for {datastack}")
            datastack_info = get_datastack_info(datastack)
            datastack_info['database_expires'] = True     
            task = run_complete_worflow.s(datastack_info, expires_in_n_days=expires_in_n_days)
            task.apply_async()
        except Exception as e:
            raise e
    return True


if __name__ == "__main__":

    run_workflow = run_periodic_materialzation()
