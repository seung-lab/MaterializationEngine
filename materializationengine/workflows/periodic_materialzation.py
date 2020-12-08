"""
Create frozen dataset.
"""
from typing import List
from typing import Optional
from os import environ

from .complete_workflow import run_complete_worflow


def _get_datastacks() -> List:
    """
    """
    datastacks = []
    
    return datastacks


def run_periodic_materialzation(datastacks: List) -> None:
    """
    """
    for datastack in datastacks:
        try:
            print(f"Start periodic materialziation job for {datastack}")
            task = run_complete_worflow.s(datastack)
            task.apply_async()
        except Exception as e:
            raise e

if __name__ == "__main__":

    run_periodic_materialzation(_get_datastacks())



