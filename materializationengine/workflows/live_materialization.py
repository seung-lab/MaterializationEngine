
import datetime
import logging
import materializationengine
import numpy as np
from annotationframeworkclient.annotationengine import AnnotationClient
from annotationframeworkclient.infoservice import InfoServiceClient
from celery import group
from flask import current_app
from sqlalchemy import create_engine, MetaData, Table, inspect
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.sql import func, or_
from sqlalchemy.engine.url import make_url
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.automap import automap_base

import cloudvolume
from celery import group, chain, chord, subtask, signature
from celery.result import AsyncResult

# from dynamicannotationdb.client import AnnotationDBMeta
from emannotationschemas import models as em_models
from emannotationschemas import get_schema
from emannotationschemas.flatten import create_flattened_schema
from emannotationschemas.models import Base, create_table_dict, format_version_db_uri
from dynamicannotationdb.key_utils import (
    build_table_id,
    build_segmentation_table_id,
    get_table_name_from_table_id,
)
from materializationengine import materializationmanager as manager
from materializationengine import materialize
from materializationengine.celery_worker import celery
from materializationengine.database import get_db
from materializationengine.extensions import create_session
from materializationengine.models import AnalysisMetadata, Base
from materializationengine.errors import AnnotationParseFailure, TaskFailure
from materializationengine.chunkedgraph_gateway import ChunkedGraphGateway
from materializationengine.utils import make_root_id_column_name, build_materailized_table_id
from typing import List
from copy import copy, deepcopy

BIGTABLE = current_app.config["BIGTABLE_CONFIG"]
PROJECT_ID = BIGTABLE["project_id"]
CG_INSTANCE_ID = BIGTABLE["instance_id"]
AMDB_INSTANCE_ID = BIGTABLE["amdb_instance_id"]
CHUNKGRAPH_TABLE_ID = current_app.config["CHUNKGRAPH_TABLE_ID"]
INFOSERVICE_ADDRESS = current_app.config["INFOSERVICE_ENDPOINT"]
ANNO_ADDRESS = current_app.config["ANNO_ENDPOINT"]

"""
Materialization Modes

    Live segmentation:

        Iterate through an aligned_volume database:
            for each annotation table:
                get max_id
                if max_id < some threshold
                    get SegmentationModel
                    parse supervoxel columns {prefix}_supervoxel_id
                        chunk supervoxel ids for root_id lookup:
                            get all expired root_ids between now and last update per table
                            send list of ids to be query chunkedgraph for root_ids
                            return root_ids associated with supervoxel ids
                        find root_id columns associated with supervoxel id columns:
                            update or insert root_ids
                            update_last updated time_stamp in segmentation metadata
                    update root_id rows

    query db for NULL root_id rows
                {prefix}_missing_root_ids = session.query.(SegmentationModel).filter((SegmentationModel.{prefix}_root_id.is_(None)) 
                send list of ids to be query pcg for root_ids

"""

def start_materialization(aligned_volume: str):
    """[summary]

    Parameters
    ----------
    aligned_volume : str
        [description]

    Returns
    -------
    [type]
        [description]
    """    

    # this is bad!
    result = aligned_volume_tables_task.s(aligned_volume).delay()
    tables_to_update = result.get()
    for table_info in tables_to_update:
        result = chunk_supervoxel_ids_task.s(table_info).delay()
        chunk_info = result.get()
        for chunk in chunk_info:
            result = get_supervoxel_ids_task.s(table_info, chunk).delay()
            supervoxel_ids = result.get()
            get_root_ids_task.s(supervoxel_ids, table_info)

@celery.task(name='threads:collect')
def collect(*args, **kwargs):
    return args, kwargs

@celery.task(name="threads:chunk_supervoxel_ids")
def chunk_supervoxel_ids(*args, **kwargs):
    if args:
        # tasks = [chunk_supervoxel_ids_task.s(tables) for table in tables]
        tasks = [chunk_supervoxel_ids_task.s(args[0])]
        return chord(group(tasks), collect.s())
    else:
        raise TaskFailure("No data to process")

@celery.task(name="threads:noop")
def noop(*args, **kwargs):
    # Task accepts any arguments and does nothing
    print(args, kwargs)
    return kwargs


@celery.task(name="threads:dmap")
def dmap(args_iter, celery_task):
    """
    Takes an iterator of argument tuples and queues them up for celery to run with the function.
    """
    callback = subtask(celery_task)
    for arg in args_iter:
        logging.info(f"ARRRGGGGG {arg}")
    run_in_parallel = group(clone_signature(callback, args=args) for args in args_iter)
    return run_in_parallel


def clone_signature(sig, args=(), kwargs=(), **opts):
    """
    Turns out that a chain clone() does not copy the arguments properly - this
    clone does.
    From: https://stackoverflow.com/a/53442344/3189
    """
    if sig.subtask_type and sig.subtask_type != "chain":
        raise NotImplementedError(
            "Cloning only supported for Tasks and chains, not {}".format(sig.subtask_type)
        )
    clone = sig.clone()
    if hasattr(clone, "tasks"):
        task_to_apply_args_to = clone.tasks[0]
    else:
        task_to_apply_args_to = clone
    args, kwargs, opts = task_to_apply_args_to._merge(args=args, kwargs=kwargs, options=opts)
    task_to_apply_args_to.update(args=args, kwargs=kwargs, options=deepcopy(opts))
    return clone


def lookup_sv_and_cg_bsp(cg, supervoxel_id, item, pixel_ratios=(1.0, 1.0, 1.0), time_stamp=None):
    """[summary]

    Parameters
    ----------
    cg : pychunkedgraph.ChunkedGraph
        chunkedgraph instance to use to lookup supervoxel and root_ids
    supervoxel_id : int
        id of supervoxel for root_id lookup
    item : dict
        deserialized boundspatialpoint to process
    pixel_ratios : tuple, optional
        ratios to multiple position coordinates
        to get cg.cv segmentation coordinates, by default (1.0, 1.0, 1.0)
    time_stamp : [datatime], optional
        time_stamp to lock to, by default None

    Raises
    ------
    AnnotationParseFailure
    """
    if time_stamp is None:
        raise AnnotationParseFailure("passing no timestamp")

    try:
        voxel = np.array(item["position"]) * np.array(pixel_ratios)
    except Exception as e:
        msg = f"Error: failed to lookup sv_id of voxel {voxel}. reason {e}"
        raise AnnotationParseFailure(msg)

    if supervoxel_id == 0:
        root_id = 0
    else:
        try:
            root_id = cg.get_root(supervoxel_id, time_stamp=time_stamp)
        except Exception as e:
            msg = f"Error: failed to lookup root_id of sv_id {supervoxel_id} {e}"
            raise AnnotationParseFailure(msg)

    item["supervoxel_id"] = int(supervoxel_id)
    item["root_id"] = int(root_id)

@celery.task(name="threads:aligned_volume_tables_task")
def aligned_volume_tables_task(aligned_volume: str) -> List[str]:
    """Initialize materialization by aligned volume. Iterates
    thorugh all tables and checks if they are below a table row count
    size. The list of tables are passed to workers for root_id updating
    based on supervoxel_id value and timestamp.

    Parameters
    ----------
    aligned_volume : str
        [description]

    Returns
    -------
    List[str]
        [description]
    """
    db = get_db(aligned_volume)
    segmentation_table_ids = db.get_existing_segmentation_table_ids()
    tables_to_update = []
    table_info = {}
    for seg_table_id in segmentation_table_ids:
        max_id = db._get_table_row_count(seg_table_id)
        table_info['max_id'] = int(max_id)
        table_info['segmentation_table_id'] = str(seg_table_id)
        table_info['aligned_volume'] = str(aligned_volume)
        tables_to_update.append(table_info.copy())
    return tables_to_update

@celery.task(name="threads:chunk_supervoxel_ids_task")
def chunk_supervoxel_ids_task(segmentation_data: dict,
                              block_size: int = 2500) -> dict:
    """Get supervoxel ids from an annotation table. Chunks the supervoxel ids
    into list of length 'block_size'.

    Parameters
    ----------
    segmentation_table_info: dict
        name of database to target
    block_size : int, optional
        block size to chunk queries into, by default 2500

    Returns
    -------
    List[int]
        list of supervoxel ids
    """

    max_id = segmentation_data.get('max_id')
    n_parts = int(max(1, max_id / block_size))
    n_parts += 1
    id_chunks = np.linspace(1, max_id, num=n_parts, dtype=np.int64).tolist()
    chunk_ends = []
    for i_id_chunk in range(len(id_chunks) - 1):
        chunk_ends.append([id_chunks[i_id_chunk], id_chunks[i_id_chunk + 1]])
    return chunk_ends

@celery.task(name="threads:get_supervoxel_ids_task")
def get_supervoxel_ids_task(segmentation_data: dict,
                            chunks: List[int]) -> List[int]:
    """Iterates over columns with 'supervoxel_id' present in the name and
    returns supervoxel ids between start and stop ids.

    Parameters
    ----------
    segmentation_table_info: dict
        name of database to target
    start_id : int
        start index for querying
    end_id : int
        end index for querying

    Returns
    -------
    List[int]
        list of supervoxel ids between 'start_id' and 'end_id'
    """
    seg_table_id = segmentation_data.get('segmentation_table_id')
    aligned_volume = segmentation_data.get('aligned_volume')
    db = get_db(aligned_volume)
    SegmentationModel = db._get_model_from_table_id(seg_table_id)
    mat_session = db.cached_session
    try:
        logging.info(f"MODEL: {SegmentationModel}")

        columns = [column.name for column in SegmentationModel.__table__.columns]
        supervoxel_id_columns = [column for column in columns if 'supervoxel_id' in column]
        logging.info(f"COLUMNS: {supervoxel_id_columns}")

        supervoxel_id_data = {}
        for supervoxel_id_column in supervoxel_id_columns:
            supervoxel_id_data[supervoxel_id_column] = [
                data
                for data in mat_session.query(getattr(SegmentationModel, supervoxel_id_column)).filter(
                    or_(SegmentationModel.annotation_id).between(int(chunks[0]), int(chunks[1]))
                )
            ]
            mat_session.close()
        return supervoxel_id_data
    except Exception as e:
        raise(e)

@celery.task(name="threads:get_root_ids_task")
def get_root_ids_task(supervoxel_data: dict,
                      segmentation_data: List[int],
                      time_stamp: datetime.datetime,
                      pixel_ratios=[4, 4, 40]):
    """[summary]

    Parameters
    ----------
    supervoxel_data : dict
        name of database to target
    segmentation_data : list
        list of supervoxel ids for root_id lookup
    end_id : int
        [description]
    time_stamp : datetime.datetime
        [description]

    Returns
    -------
    [type]
        [description]
    """
    pcg_table_name = segmentation_data.get('pcg_table_name')
    cg = ChunkedGraphGateway(pcg_table_name)

    if time_stamp is None:
        time_stamp = (datetime.datetime.utcnow() - datetime.timedelta(minutes=5))

    root_id_data = {}
    try:
        for column_name, supervoxels in supervoxel_data.items():
            supervoxel_data = np.asarray(supervoxels, dtype=np.uint64)
            col = make_root_id_column_name(column_name)
            root_ids = cg.get_roots(supervoxel_data[:, 1], time_stamp=time_stamp)
            root_id_data[col] = root_ids
    except Exception as e:
        raise AnnotationParseFailure(e)

    return root_id_data

def update_root_ids(segmentation_data: dict,
                    start_id: int,
                    end_id: int,
                    time_stamp: datetime.datetime):

    seg_table_id = segmentation_data.get('segmentation_table_id')
    aligned_volume = segmentation_data.get('aligned_volume')
    db = get_db(aligned_volume)
    mat_session = db.cached_session

    try:
        for supervoxel_id_column in supervoxel_id_columns:
            supervoxel_id_data[supervoxel_id_column] = [
                data
                for data in mat_session.query(getattr(SegmentationModel, supervoxel_id_column)).filter(
                    or_(SegmentationModel.annotation_id).between(int(chunks[0]), int(chunks[1]))
                )
            ]
    except Exception as e:
        raise e



def update_segment_ids():
    raise NotImplementedError

