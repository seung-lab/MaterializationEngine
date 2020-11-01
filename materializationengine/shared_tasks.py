from typing import List
import datetime
from dynamicannotationdb.models import SegmentationMetadata

from celery import Task, chain, chord, chunks, group, subtask
from celery.utils.log import get_task_logger
from flask import current_app
from sqlalchemy import MetaData, and_, create_engine, func, text
from emannotationschemas import models as em_models
from materializationengine.celery_worker import celery
from materializationengine.database import sqlalchemy_cache
from materializationengine.utils import create_annotation_model

ROW_CHUNK_SIZE = current_app.config["MATERIALIZATION_ROW_CHUNK_SIZE"]

celery_logger = get_task_logger(__name__)


# @celery.task(name="process:chunk_supervoxel_ids_task", bind=True, acks_late=True)
def chunk_supervoxel_ids_task(mat_metadata: dict) -> List[List]:
    """Creates list of chunks with start:end index for chunking queries for materialziation.

    Parameters
    ----------
    mat_metadata : dict
        Materialization metadata

    Returns
    -------
    List[List]
        list of list containg start and end indices
    """
    AnnotationModel = create_annotation_model(mat_metadata)
    chunk_size = mat_metadata.get('chunk_size', None)
    
    if not chunk_size:
        chunk_size = ROW_CHUNK_SIZE

    chunked_ids = chunk_ids(mat_metadata, AnnotationModel.id, chunk_size)

    return [chunk for chunk in chunked_ids]

@celery.task(name="process:fin", acks_late=True, bind=True)
def fin(self, *args, **kwargs):
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


@celery.task(name="process:update_metadata",
             bind=True,
             acks_late=True,
             autoretry_for=(Exception,),
             max_retries=3)
def update_metadata(self, *args):
    status, mat_metadata = args
    aligned_volume = mat_metadata['aligned_volume']
    segmentation_table_name = mat_metadata['segmentation_table_name']

    session = sqlalchemy_cache.get(aligned_volume)
    
    materialization_time_stamp = mat_metadata['materialization_time_stamp']
    last_updated_time_stamp = datetime.datetime.strptime(materialization_time_stamp, '%Y-%m-%dT%H:%M:%S.%f')

    try:
        seg_metadata = session.query(SegmentationMetadata).filter(
            SegmentationMetadata.table_name == segmentation_table_name).one()
        seg_metadata.last_updated = last_updated_time_stamp
        session.commit()
    except Exception as e:
        celery_logger.error(f"SQL ERROR: {e}")
        session.rollback()
    finally:
        session.close()
    return {f"Table: {segmentation_table_name}": f"Time stamp {materialization_time_stamp}"}

