import datetime
from typing import List

from celery import chain, chord, group
from celery.utils.log import get_task_logger
from materializationengine.celery_init import celery
from materializationengine.shared_tasks import (chunk_annotation_ids, fin,
                                                get_materialization_info,
                                                update_metadata)
from materializationengine.workflows.create_frozen_database import (
    add_indices, check_tables, create_analysis_database,
    create_materialized_metadata, create_new_version,
    drop_tables, merge_tables, update_table_metadata)
from materializationengine.workflows.ingest_new_annotations import (
    create_missing_segmentation_table, get_materialization_info,
    ingest_new_annotations)
from materializationengine.workflows.update_root_ids import (
    get_expired_root_ids, update_root_ids)

celery_logger = get_task_logger(__name__)


@celery.task(name="process:run_complete_worflow")
def run_complete_worflow(datastack_info: dict, days_to_expire: int = 5):
    """Run complete materialziation workflow. 
    Workflow overview:
        - Find all annotations with missing segmentation rows 
        and lookup supervoxel_id and root_id
        - Lookup all expired root_ids and update them
        - Copy the database to a new versioned database
        - Merge annotation and segmentation tables

    Args:
        datastack_info (dict): [description]
        days_to_expire (int, optional): [description]. Defaults to 5.
    """
    materialization_time_stamp = datetime.datetime.utcnow()

    new_version_number = create_new_version(
        datastack_info, materialization_time_stamp, days_to_expire)

    mat_info = get_materialization_info(
        datastack_info, new_version_number, materialization_time_stamp)
    celery_logger.info(mat_info)

    update_live_database_workflow = []

    # lookup missing segmentation data for new annotations and update expired root_ids
    for mat_metadata in mat_info:
        annotation_chunks = chunk_annotation_ids(mat_metadata)
        chunked_roots = get_expired_root_ids(mat_metadata)
        if mat_metadata['row_count'] < 1_000_000 and mat_metadata['create_segmentation_table']:
            new_annotations = True
            new_annotation_workflow = ingest_new_annotations_workflow(
                mat_metadata, annotation_chunks)  # return here is required for chords

        else:
            new_annotations = False

        update_expired_roots_workflow = update_root_ids_workflow(
            mat_metadata, chunked_roots)
        if new_annotations:
            ingest_and_freeze_workflow = chain(
                new_annotation_workflow, update_expired_roots_workflow)
            update_live_database_workflow.append(ingest_and_freeze_workflow)
        else:
            update_live_database_workflow.append(update_expired_roots_workflow)
    # copy live database as a materialized version and drop uneeded tables
    setup_versioned_database_workflow = create_materializied_database_workflow(
        datastack_info, new_version_number, materialization_time_stamp, mat_info)

    # drop indices, merge annotation and segmentation tables and re-add indices on merged table
    format_database_workflow = format_materialization_database_workflow(
        mat_info)

    final_workflow = chain(
        chord(update_live_database_workflow, fin.si()),
        setup_versioned_database_workflow,
        chord(format_database_workflow, fin.si()),
        check_tables.si(mat_info, new_version_number)
    )
    final_workflow.apply_async()


def ingest_new_annotations_workflow(mat_metadata: dict, annotation_chunks: List[int]):
    """Celery workflow to ingest new annotations. In addtion, it will 
    create missing segmentation data table if it does not exist. 
    Returns celery chain primative.

    Args:
        mat_metadata (dict): datastack info for the aligned_volume derived from the infoservice
        annotation_chunks (List[int]): list of annotation primary key ids 

    Returns:
        chain: chain of celery tasks 
    """
    new_annotation_workflow = chain(
        create_missing_segmentation_table.si(mat_metadata),
        chord([
            chain(
                ingest_new_annotations.si(mat_metadata, annotation_chunk),
            ) for annotation_chunk in annotation_chunks],
            fin.si()))  # return here is required for chords
    return new_annotation_workflow


def update_root_ids_workflow(mat_metadata: dict, chunked_roots: List[int]):
    """Celery workflow that updates expired root ids in a
    segmentation table. 

    Workflow:
        - Lookup supervoxel id associated with expired root id
        - Lookup new root id for the supervoxel
        - Update database row with new root id

    Once all root ids in a given table are updated the associated entry in the
    metadata data will also be updated.

    Args:
        mat_metadata (dict): datastack info for the aligned_volume derived from the infoservice
        chunked_roots (List[int]): chunks of expired root ids to lookup

    Returns:
        chain: chain of celery tasks 
    """
    update_expired_roots_workflow = chain(
        chord([
            group(update_root_ids(root_ids, mat_metadata))
            for root_ids in chunked_roots],
            fin.si()),
        update_metadata.si(mat_metadata),
    )
    return update_expired_roots_workflow


def create_materializied_database_workflow(datastack_info: dict,
                                           new_version_number: int,
                                           materialization_time_stamp: datetime.datetime.utcnow,
                                           mat_info: dict):
    """Celery workflow to create a materializied database.
    Workflow:
        - Copy live database as a versioned materialized database.
        - Create materialziation metadata table and populate.
        - Drop tables that are uneeded in the materialized database.

    Args:
        datastack_info (dict): database information
        new_version_number (int): version number of database
        materialization_time_stamp (datetime.datetime.utcnow): 
            materialized timestamp
        mat_info (dict): materialization metadata information

    Returns:
        chain: chain of celery tasks
    """
    setup_versioned_database = chain(
        create_analysis_database.si(datastack_info, new_version_number),
        create_materialized_metadata.si(datastack_info,
                                        new_version_number,
                                        materialization_time_stamp),
        update_table_metadata.si(mat_info),
        drop_tables.si(datastack_info, new_version_number))
    return setup_versioned_database


def format_materialization_database_workflow(mat_info: dict):
    """Celery workflow to format the materialized database.
    Workflow:
        - Merge annotation and segmentation tables into
        a single table.
        - Add indexes into merged tables.

    Args:
        mat_info (dict): materialization metadata information

    Returns:
        chain: chain of celery tasks
    """
    create_frozen_database_tasks = []
    for mat_metadata in mat_info:
        create_frozen_database_workflow = chain(
            merge_tables.si(mat_metadata),
            add_indices.si(mat_metadata))
        create_frozen_database_tasks.append(create_frozen_database_workflow)
    return create_frozen_database_tasks
