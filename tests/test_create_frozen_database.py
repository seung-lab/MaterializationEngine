import datetime
import logging

from materializationengine.workflows.create_frozen_database import (
    add_indices, check_tables, create_analysis_database,
    create_materialized_metadata, create_new_version, drop_tables,
    merge_tables, update_table_metadata)

datastack_info = {
    'datastack': 'test_aligned_volume',
    'aligned_volume': {
        'name': 'test_aligned_volume'
    },
    'segmentation_source': 'graphene://https://fake-daf.com/segmentation/table/test_pcg'}
materialization_time_stamp = datetime.datetime.utcnow()


def test_create_new_version(test_app):

    new_version_number = create_new_version(
        datastack_info, materialization_time_stamp, 7)
    assert new_version_number == 1


def test_create_analysis_database():
    is_created = create_analysis_database(datastack_info, 1)
    assert is_created == True


def test_create_materialized_metadata():
    is_table_created = create_materialized_metadata(
        datastack_info=datastack_info,
        analysis_version=1,
        materialization_time_stamp=materialization_time_stamp)
    assert is_table_created == True


def test_update_table_metadata(mat_metadata):
    tables = update_table_metadata([mat_metadata])
    assert tables == ['test_synapse_table']


def test_drop_tables():
    dropped_tables = drop_tables(datastack_info, analysis_version=1)
    logging.info(dropped_tables)
    assert dropped_tables != None


def test_merge_tables(mat_metadata):
    table_info = merge_tables(mat_metadata)
    logging.info(table_info)
    assert table_info == "Number of rows copied: 3"


def test_add_indices(mat_metadata):
    index = add_indices.s(mat_metadata).apply()
    logging.info(index.get())
    assert "Index" in index.get()


def test_check_tables(mat_metadata):
    table_info = check_tables.s([mat_metadata], 1).apply()
    assert table_info.get() == "All materialized tables match valid row number from live tables"
