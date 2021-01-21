import datetime
from celery import chain, chord, group
from celery.utils.log import get_task_logger
from materializationengine.celery_worker import celery
from materializationengine.shared_tasks import (chunk_supervoxel_ids_task, fin,
                                                update_metadata,
                                                get_materialization_info)
from materializationengine.workflows.create_frozen_database import (
    create_analysis_database, create_analysis_tables, create_new_version,
    merge_tables, update_table_metadata, drop_tables, drop_indices, add_indices, check_tables)
from materializationengine.workflows.ingest_new_annotations import (
    create_missing_segmentation_table, get_materialization_info,
    ingest_new_annotations)
from materializationengine.workflows.update_root_ids import (
    get_expired_root_ids, update_root_ids)

celery_logger = get_task_logger(__name__)


@celery.task(name="process:run_complete_worflow",
             acks_late=True,
             bind=True)
def run_complete_worflow(self, datastack_info: dict, expires_in_n_days: int = 5):
    """Run complete materialziation workflow. 
    Workflow overview:
        - Find all annotations with missing segmentation rows 
        and lookup supervoxel_id and root_id
        - Lookup all expired root_ids and update them
        - Copy the database to a new versioned database
        - Merge annotation and segmentation tables

    Args:
        datastack_info (dict): [description]
        expires_in_n_days (int, optional): [description]. Defaults to 5.

    Returns:
        [type]: [description]
    """
    materialization_time_stamp = datetime.datetime.utcnow()

    new_version_number = create_new_version(datastack_info, materialization_time_stamp, expires_in_n_days)

    mat_info = get_materialization_info(datastack_info, new_version_number, materialization_time_stamp)
    celery_logger.info(mat_info)

    update_live_database_tasks = []

    # lookup missing segmentation data for new annotations and update expired root_ids
    for mat_metadata in mat_info:
        supervoxel_chunks = chunk_supervoxel_ids_task(mat_metadata)
        chunked_roots = get_expired_root_ids(mat_metadata)
        if mat_metadata['row_count'] < 1_000_000:
            new_annotation_workflow = chain(
                create_missing_segmentation_table.si(mat_metadata),
                chord([
                    chain(
                        ingest_new_annotations.si(mat_metadata, chunk),
                    ) for chunk in supervoxel_chunks],
                    fin.si()),  # return here is required for chords
                fin.si())
        else:
            new_annotation_workflow = None
    
        update_expired_roots_workflow = chain(
            chord([
                group(update_root_ids(root_ids, mat_metadata))
                   for root_ids in chunked_roots],
                   fin.si()),
            update_metadata.si(mat_metadata),
        )

        if new_annotation_workflow is not None:
            ingest_and_freeze_workflow = chain(
                new_annotation_workflow, update_expired_roots_workflow)
            update_live_database_tasks.append(ingest_and_freeze_workflow)
        else:
            update_live_database_tasks.append(update_expired_roots_workflow)

    # copy live database as a materialized version and drop uneeded tables
    setup_versioned_database = chain(create_analysis_database.si(datastack_info, new_version_number),
                                     create_analysis_tables.si(datastack_info, new_version_number, materialization_time_stamp),
                                     update_table_metadata.si(mat_info),
                                     drop_tables.si(datastack_info, new_version_number))
    
    # drop indices, merge annotation and segmentation tables and re-add indices on merged table
    create_frozen_database_tasks = []    
    for mat_metadata in mat_info:       
        create_frozen_database_workflow = chain(
            drop_indices.si(mat_metadata),
            merge_tables.si(mat_metadata),
            add_indices.si(mat_metadata))
        create_frozen_database_tasks.append(create_frozen_database_workflow)


    final_workflow = chain(
        chord(update_live_database_tasks, fin.si()),
        setup_versioned_database,
        chord(create_frozen_database_tasks, fin.si()),
        check_tables.si(mat_info, new_version_number)        
        )
    final_workflow.apply_async()
