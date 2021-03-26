
# Mock pychunkgraph imports and cloudvolume before importing
# the tasks
import sys
from unittest.mock import MagicMock

sys.modules['materializationengine.chunkedgraph_gateway'] = MagicMock()
sys.modules['cloudvolume'] = MagicMock()
import logging

import numpy as np
from materializationengine.workflows.ingest_new_annotations import (
    create_missing_segmentation_table,
    get_annotations_with_missing_supervoxel_ids,
    get_cloudvolume_supervoxel_ids, get_new_root_ids, get_sql_supervoxel_ids,
    insert_segmentation_data)
from numpy import nan


missing_segmentations_data = {'post_pt_supervoxel_id': [nan, nan, nan],
                              'pre_pt_supervoxel_id': [nan, nan, nan],
                              'post_pt_position': [
                                  [70000, 80000, 90000],
                                  [71000, 81000, 91000],
                                  [72000, 82000, 92000]
                                  ],
                              'pre_pt_position': [
                                  [10000, 20000, 30000],
                                  [11000, 21000, 31000],
                                  [12000, 22000, 32000]
                                  ],
                              'id': [1, 2, 3]}


mocked_supervoxel_data = {'post_pt_supervoxel_id': [10000000, 10000000, 10000000],
                          'pre_pt_supervoxel_id': [10000000, 10000000, 10000000],
                          'post_pt_position': [
                              [70000, 80000, 90000],
                              [71000, 81000, 91000],
                              [72000, 82000, 92000]],
                          'pre_pt_position': [
                              [10000, 20000, 30000],
                              [11000, 21000, 31000],
                              [12000, 22000, 32000]
                              ],
                          'id': [1, 2, 3]}


mocked_root_id_data = [{'post_pt_supervoxel_id': 10000000,
                        'pre_pt_supervoxel_id': 10000000,
                        'id': 1,
                        'post_pt_root_id': 20000000,
                        'pre_pt_root_id': 20000000},
                       {'post_pt_supervoxel_id': 10000000,
                        'pre_pt_supervoxel_id': 10000000,
                        'id': 2, 'post_pt_root_id': 20000000,
                        'pre_pt_root_id': 20000000},
                       {'post_pt_supervoxel_id': 10000000,
                        'pre_pt_supervoxel_id': 10000000,
                        'id': 3,
                        'post_pt_root_id': 20000000,
                        'pre_pt_root_id': 20000000}
                       ]


def test_create_missing_segmentation_table(mat_metadata, db_client):
    table_metadata = create_missing_segmentation_table(mat_metadata)
    __, engine= db_client

    seg_table_exists= engine.dialect.has_table(
        engine.connect(), "test_synapse_table__test_pcg")

    assert seg_table_exists == True


def test_get_annotations_with_missing_supervoxel_ids(mat_metadata):
    id_chunk_range = [1, 4]
    annotations = get_annotations_with_missing_supervoxel_ids(
        mat_metadata, id_chunk_range)
    assert annotations == missing_segmentations_data


def test_get_cloudvolume_supervoxel_ids(monkeypatch, mat_metadata):

    def mock_cloudvolume(*args, **kwargs):
        return np.ndarray((1,), buffer=np.array([10000000]), dtype=int)
    monkeypatch.setattr(
        "materializationengine.workflows.ingest_new_annotations.get_sv_id", mock_cloudvolume)
    supervoxel_data = get_cloudvolume_supervoxel_ids(
        missing_segmentations_data, mat_metadata)
    assert supervoxel_data == mocked_supervoxel_data


def test_get_sql_supervoxel_ids(mat_metadata):
    id_chunk_range = [1, 4]
    supervoxel_ids = get_sql_supervoxel_ids(id_chunk_range, mat_metadata)
    logging.info(supervoxel_ids)


def test_get_new_root_ids(monkeypatch, mat_metadata):
    def mock_get_roots(*args, **kwargs):
        return np.ndarray((1,), buffer=np.array([20000000]), dtype=int)
    monkeypatch.setattr(
        "materializationengine.workflows.ingest_new_annotations.get_root_ids", mock_get_roots)
    root_ids= get_new_root_ids(mocked_supervoxel_data, mat_metadata)
    logging.info(root_ids)
    assert root_ids == mocked_root_id_data

def test_insert_segmentation_data(annotation_data, mat_metadata):
    segmentation_data = annotation_data['segmentation_data']
    num_of_rows = insert_segmentation_data(segmentation_data, mat_metadata)
    assert num_of_rows == {'New segmentations inserted': 3} 

