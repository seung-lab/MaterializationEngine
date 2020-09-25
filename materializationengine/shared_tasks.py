from typing import List

from celery import Task, chain, chord, chunks, group, subtask
from celery.utils.log import get_task_logger
from flask import current_app
from sqlalchemy import MetaData, and_, create_engine, func, text
from emannotationschemas import models as em_models
from materializationengine.celery_worker import celery
from materializationengine.database import sqlalchemy_cache
from materializationengine.utils import create_annotation_model

ROW_CHUNK_SIZE = current_app.config["MATERIALIZATION_ROW_CHUNK_SIZE"]


@celery.task(name="process:chunk_supervoxel_ids_task", bind=True)
def chunk_supervoxel_ids_task(self, mat_metadata: dict, chunk_size: int = None) -> List[List]:
    """Creates list of chunks with start:end index for chunking queries for materialziation.

    Parameters
    ----------
    mat_metadata : dict
        Materialization metadata
    block_size : int, optional
        [description], by default 2500

    Returns
    -------
    List[List]
        list of list containg start and end indices
    """
    AnnotationModel = create_annotation_model(mat_metadata)

    if not chunk_size:
        chunk_size = ROW_CHUNK_SIZE

    chunked_ids = chunk_ids(mat_metadata, AnnotationModel.id, chunk_size)

    return [chunk for chunk in chunked_ids]

@celery.task(name="process:fin", acks_late=True)
def fin(*args, **kwargs):
    return True


@celery.task(name="process:collect_data", acks_late=True)
def collect_data(*args, **kwargs):
    return args, kwargs

def query_id_range(column, start_id: int, end_id: int):
    if end_id:
        return and_(column >= start_id, column < end_id)
    else:
        return column >= start_id


def chunk_ids(mat_metadata, model, chunk_size: int):
    aligned_volume = mat_metadata.get('aligned_volume')
    session = sqlalchemy_cache.get(aligned_volume)

    q = session.query(
        model, func.row_number().over(order_by=model).label("row_count")
    ).from_self(model)

    if chunk_size > 1:
        q = q.filter(text("row_count %% %d=1" % chunk_size))

    chunks = [id for id, in q]

    while chunks:
        chunk_start = chunks.pop(0)
        if chunks:
            chunk_end = chunks[0]
        else:
            chunk_end = None
        yield [chunk_start, chunk_end]