import datetime
from typing import List

import numpy as np
import pandas as pd
from celery import chain, chord, group
from celery.utils.log import get_task_logger
from dynamicannotationdb.key_utils import build_segmentation_table_name
from dynamicannotationdb.models import SegmentationMetadata
from materializationengine.celery_worker import celery
from materializationengine.chunkedgraph_gateway import chunkedgraph_cache
from materializationengine.database import get_db, sqlalchemy_cache
from materializationengine.shared_tasks import fin, update_metadata
from materializationengine.upsert import upsert
from materializationengine.utils import create_segmentation_model
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import func, or_

celery_logger = get_task_logger(__name__)


@celery.task(name="process:update_root_ids_task",
             bind=True,
             acks_late=True,)
def expired_root_id_workflow(self, datastack_info: dict):
    """Workflow to process expired root ids and lookup and
    update table with current root ids.

    Args:
        datastack_info (dict): Workflow metadata
    """
    aligned_volume_name = datastack_info['aligned_volume']['name']
    pcg_table_name = datastack_info['segmentation_source'].split("/")[-1]

    mat_info = get_materialization_info(aligned_volume_name, pcg_table_name)

    for mat_metadata in mat_info:
        if mat_metadata:
            chunked_roots = get_expired_root_ids(mat_metadata)
            process_root_ids = chain(
                chord([chain(update_root_ids.si(root, mat_metadata))
                       for root in chunked_roots], fin.si()),  # return here is required for chords
                update_metadata.si(mat_metadata))  # final task which will process a return status/timing etc...
            process_root_ids.apply_async()


@celery.task(name="process:update_root_ids",
             acks_late=True,
             bind=True,)
def update_root_ids(self, root_ids: List[int], mat_metadata: dict) -> True:
    """Chunks supervoxel data and distributes 
    supervoxels ids to parallel worker tasks that
    lookup new root_ids and update the database table.

    Args:
        root_ids (List[int]): List of expired root_ids 
        mat_metadata (dict): metadata for tasks
    """
    supervoxel_data = get_supervoxel_ids(root_ids, mat_metadata)
    for __, data in supervoxel_data.items():
        if data:
            chunked_supervoxels = create_chunks(data, 100)
            worker_chain = group(get_new_roots.s(chunk, mat_metadata) for chunk in chunked_supervoxels)
            worker_chain.apply_async()
    return True

def create_chunks(data_list: List, chunk_size: int):
    """Return successive n-sized chunks from a list."""
    if len(data_list) <= chunk_size:
        chunk_size = len(data_list)
    for i in range(0, len(data_list), chunk_size):
        yield data_list[i:i + chunk_size]


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
            'materialization_time_stamp': str(materialization_time_stamp)
        }
        metadata.append(table_metadata.copy())
    db.cached_session.close()
    return metadata


def get_expired_root_ids(mat_metadata: dict, expired_chunk_size: int = 10):
    """[summary]

    Args:
        mat_metadata (dict): [description]
        expired_chunk_size (int, optional): [description]. Defaults to 10.

    Returns:
        [type]: [description]

    Yields:
        [type]: [description]
    """
    last_updated_ts = mat_metadata.get('last_updated_time_stamp', None)
    pcg_table_name = mat_metadata.get("pcg_table_name")

    if last_updated_ts:
        last_updated_ts = datetime.datetime.strptime(
            last_updated_ts, '%Y-%m-%d %H:%M:%S.%f')
    else:
        last_updated_ts = datetime.datetime.utcnow() - datetime.timedelta(minutes=60)

    cg = chunkedgraph_cache.init_pcg(pcg_table_name)

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

        for x in chunked_root_ids:
            yield x.tolist()
    else:
        return None


def get_supervoxel_ids(root_id_chunk: list, mat_metadata: dict):
    """Get supervoxel ids associated with expired root ids

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
        raise e
    finally:
        session.close()
    return expired_root_id_data


@celery.task(name="process:get_new_roots",
             acks_late=True,
             bind=True,
             autoretry_for=(Exception,),
             max_retries=3)
def get_new_roots(self, supervoxel_chunk: list, mat_metadata: dict):
    """Get new roots from supervoxels ids of expired roots

    Args:
        supervoxel_chunk (list): [description]
        mat_metadata (dict): [description]

    Returns:
        [dict]: dicts of new root_ids
    """
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

    cg = chunkedgraph_cache.init_pcg(pcg_table_name)
    root_id_array = np.squeeze(cg.get_roots(
        supervoxel_data, time_stamp=formatted_mat_ts))

    del supervoxel_data

    root_ids_df.loc[supervoxel_df.index,
                    root_id_col_name[0]] = root_id_array
    root_ids_df.drop(columns=[supervoxel_col_name[0]])

    SegmentationModel = create_segmentation_model(mat_metadata)
    aligned_volume = mat_metadata.get("aligned_volume")

    session = sqlalchemy_cache.get(aligned_volume)
    try:
        session.bulk_update_mappings(SegmentationModel, root_ids_df.to_dict(orient='records'))
        session.commit()
    except Exception as e:
        celery_logger.error(f"ERROR: {e}")
    finally:
        session.close()
    return True