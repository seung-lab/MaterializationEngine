import datetime
import logging
import time
from copy import deepcopy
from functools import lru_cache, partial
from itertools import groupby, islice, repeat, takewhile
from operator import itemgetter
from typing import List

import cloudvolume
import numpy as np
import pandas as pd
from celery import Task, chain, chord, chunks, group, subtask
from celery.utils.log import get_task_logger
from dynamicannotationdb.key_utils import build_segmentation_table_name
from dynamicannotationdb.models import SegmentationMetadata
from emannotationschemas import models as em_models
from flask import current_app
from materializationengine.celery_worker import celery
from materializationengine.chunkedgraph_gateway import ChunkedGraphGateway
from materializationengine.database import (create_session, get_db,
                                            sqlalchemy_cache)
from materializationengine.errors import (AnnotationParseFailure, TaskFailure,
                                          WrongModelType)
from materializationengine.shared_tasks import (chunk_supervoxel_ids_task, fin,
                                                query_id_range,
                                                update_metadata)
from materializationengine.upsert import upsert
from materializationengine.utils import (create_annotation_model,
                                         create_segmentation_model,
                                         get_geom_from_wkb,
                                         get_query_columns_by_suffix,
                                         make_root_id_column_name)
from sqlalchemy import MetaData, and_, create_engine, func, text
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import or_

celery_logger = get_task_logger(__name__)


@celery.task(name="process:update_root_ids_task",
             bind=True)
def update_root_ids_task(self, datastack_info: dict):
    """[summary]

    Args:
        datastack_info (dict): [description]

    Returns:
        [type]: [description]
    """
    aligned_volume_name = datastack_info['aligned_volume']['name']
    pcg_table_name = datastack_info['segmentation_source'].split("/")[-1]

    mat_info = get_materialization_info(aligned_volume_name, pcg_table_name)

    for mat_metadata in mat_info:
        if mat_metadata:
            expired_root_ids = get_expire_root_ids(mat_metadata)
            if expired_root_ids:
                process_root_ids = [chain(
                    get_supervoxel_ids.s(root, mat_metadata),
                    chunk_process_root_ids.s())
                    for root in expired_root_ids]
                get_new_roots_workflow = chain(
                    group(*process_root_ids), fin.si(), update_metadata.s(mat_metadata))

                get_new_roots_workflow.apply_async()
            else:
                last_updated_ts = mat_metadata.get(
                    'last_updated_time_stamp', None)
                current_time_stamp = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)

                return {f"Nothing to update, no expired root ids between {last_updated_ts} and {current_time_stamp}"}


def create_chunks(lst, n):
    """Return successive n-sized chunks from a list."""
    if len(lst) <= n:
        n = len(lst)
    return [lst[i:i + n] for i in range(0, len(lst), n)]


@celery.task(name="process:chunk_process_root_ids",
             acks_late=True,
             bind=True,
             autoretry_for=(Exception,),
             max_retries=1)
def chunk_process_root_ids(self, supervoxel_data_and_mat_metadata):
    """Create parallel chains to lookup new root_ids and insert 
    into database.

    Args:
        mat_metadata (dict): [description]
        supervoxel_data (dict): [description]

    Returns:
        [type]: [description]
    """
    supervoxel_data, mat_metadata = supervoxel_data_and_mat_metadata
    try:
        for __, data in supervoxel_data.items():
            if data:
                chunked_supervoxels = create_chunks(data, 100)
                worker_chain = [chain(get_new_roots.s(chunk, mat_metadata), update_segmentation_table.s(mat_metadata)
                                      ) for chunk in chunked_supervoxels]
                return group(*worker_chain).apply_async()
    except Exception as e:
        celery_logger.error(e)
        raise self.retry(exc=e, countdown=3)


def get_materialization_info(aligned_volume: str,
                             pcg_table_name: str) -> List[dict]:
    """Initialize materialization by an aligned volume name. Iterates thorugh all
    tables in a aligned volume database and gathers metadata for each table. The list
    of tables are passed to workers for materialization.

    Parameters
    ----------
    aligned_volume : str
        name of aligned volume
    pcg_table_name: str
        cg_table_name

    Returns
    -------
    List[dict]
        list of dicts containing metadata for each table
    """
    db = get_db(aligned_volume)
    annotation_tables = db.get_valid_table_names()
    metadata = []
    for annotation_table in annotation_tables:
        segmentation_table_name = build_segmentation_table_name(
            annotation_table, pcg_table_name)

        result = db.cached_session.query(SegmentationMetadata.last_updated).filter(
            SegmentationMetadata.table_name == segmentation_table_name).first()

        materialization_time_stamp = datetime.datetime.utcnow()

        table_metadata = {
            'aligned_volume': str(aligned_volume),
            'schema': db.get_table_schema(annotation_table),
            'annotation_table_name': annotation_table,
            'segmentation_table_name': segmentation_table_name,
            'pcg_table_name': pcg_table_name,
            'last_updated_time_stamp': str(result[0]),
            'materialization_time_stamp': materialization_time_stamp,
        }
        metadata.append(table_metadata.copy())
    db.cached_session.close()
    return metadata


def get_expire_root_ids(mat_metadata, expired_chunk_size: int = 100):
    last_updated_ts = mat_metadata.get('last_updated_time_stamp', None)
    pcg_table_name = mat_metadata.get("pcg_table_name")

    if last_updated_ts:
        last_updated_ts = datetime.datetime.strptime(
            last_updated_ts, '%Y-%m-%d %H:%M:%S.%f')
    else:
        last_updated_ts = datetime.datetime.utcnow() - datetime.timedelta(minutes=60)

    cg = ChunkedGraphGateway(pcg_table_name)

    current_time_stamp = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)
    old_roots, __ = cg.get_proofread_root_ids(
        last_updated_ts, current_time_stamp)
    is_empty = np.all((old_roots == []))

    if not is_empty:
        if len(old_roots) < expired_chunk_size:
            chunks = len(old_roots)
        else:
            chunks = len(old_roots) // expired_chunk_size

        chunked_root_ids = np.array_split(old_roots, chunks)
        expired_chunked_root_id_lists = [
            x.tolist() for x in [*chunked_root_ids]]
        return expired_chunked_root_id_lists
    else:
        return None


@celery.task(name="process:get_supervoxel_ids",
             acks_late=True,
             bind=True,
             autoretry_for=(Exception,),
             max_retries=3)
def get_supervoxel_ids(self, root_id_chunk: list, mat_metadata: dict):
    """[summary]

    Args:
        root_id_chunk (list): [description]
        mat_metadata (dict): [description]

    Returns:
        [type]: [description]
    """
    aligned_volume = mat_metadata.get("aligned_volume")
    SegmentationModel = create_segmentation_model(mat_metadata)

    session = sqlalchemy_cache.get(aligned_volume)
    columns = [column.name for column in SegmentationModel.__table__.columns]
    root_id_columns = [column for column in columns if "root_id" in column]
    expired_root_id_data = {}
    try:
        for root_id_column in root_id_columns:
            prefix = root_id_column.rsplit('_', 2)[0]
            supervoxel_name = f"{prefix}_supervoxel_id"

            supervoxels = [
                data for data in session.query(SegmentationModel.id,
                                               getattr(SegmentationModel,
                                                       root_id_column),
                                               getattr(SegmentationModel, supervoxel_name)).
                filter(or_(getattr(SegmentationModel, root_id_column)).in_(
                    root_id_chunk))
            ]
            expired_root_id_data[root_id_column] = pd.DataFrame(
                supervoxels).to_dict(orient="records")
    except Exception as e:
        celery_logger.error(e)
        raise self.retry(exc=e, countdown=3)
    finally:
        session.close()
    return expired_root_id_data, mat_metadata


@celery.task(name="process:get_new_roots",
             acks_late=True,
             bind=True,
             autoretry_for=(Exception,),
             max_retries=3)
def get_new_roots(self, supervoxel_chunk: list, mat_metadata: dict):
    """[summary]

    Args:
        supervoxel_chunk (list): [description]
        mat_metadata (dict): [description]

    Returns:
        [type]: [description]
    """
    start = time.time()
    pcg_table_name = mat_metadata.get("pcg_table_name")

    materialization_time_stamp = mat_metadata['materialization_time_stamp']
    try:
        formatted_mat_ts = datetime.datetime.strptime(
            materialization_time_stamp, '%Y-%m-%dT%H:%M:%S.%f')
    except:
        formatted_mat_ts = datetime.datetime.strptime(
            materialization_time_stamp, '%Y-%m-%d %H:%M:%S.%f')
    root_ids_df = pd.DataFrame(supervoxel_chunk, dtype=object)

    supervoxel_col_name = list(
        root_ids_df.loc[:, root_ids_df.columns.str.endswith("supervoxel_id")])
    root_id_col_name = list(
        root_ids_df.loc[:, root_ids_df.columns.str.endswith("root_id")])
    supervoxel_df = root_ids_df.loc[:, supervoxel_col_name[0]]
    supervoxel_data = supervoxel_df.to_list()

    cg = ChunkedGraphGateway(pcg_table_name)
    try:
        root_id_array = np.squeeze(cg.get_roots(
            supervoxel_data, time_stamp=formatted_mat_ts))

        root_ids_df.loc[supervoxel_df.index,
                        root_id_col_name[0]] = root_id_array
        root_ids_df.drop(columns=[supervoxel_col_name[0]])
    except Exception as e:
        celery_logger.error(e)
        raise self.retry(exc=e, countdown=3)
    finally:
        celery_logger.info(time.time() - start)

    return root_ids_df.to_dict(orient='records')


@celery.task(name="process:update_segmentation_table",
             acks_late=True,
             bind=True,
             autoretry_for=(Exception,),
             max_retries=3)
def update_segmentation_table(self, materialization_data: dict, mat_metadata: dict) -> dict:
    """[summary]

    Args:
        materialization_data (dict): [description]
        mat_metadata (dict): [description]

    Returns:
        dict: [description]
    """

    if not materialization_data:
        return {'status': 'empty'}

    SegmentationModel = create_segmentation_model(mat_metadata)
    aligned_volume = mat_metadata.get("aligned_volume")

    try:
        session = sqlalchemy_cache.get(aligned_volume)
        upsert(session, materialization_data, SegmentationModel)
        session.close()
        return {'status': 'updated'}
    except SQLAlchemyError as e:
        celery_logger.error(e)
        raise self.retry(exc=e, countdown=3)
