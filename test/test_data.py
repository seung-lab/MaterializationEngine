import numpy as np
import pytest

from pychunkedgraph.backend import chunkedgraph

from materializationengine.materialize import materialize_all_annotations, materialize_root_ids
import materializationengine.materialize
from annotationengine.annotation import collect_bound_spatial_points, import_annotation_func, get_schema_from_service
from emannotationschemas.blueprint_app import get_type_schema
from mock import patch, Mock, MagicMock
import requests_mock
import numpy as np
import datetime

@pytest.fixture(scope="session")
def test_data(test_annon_dataset):
    """
    Create graph with edges as depicted below
    where there is an synapse annotation that connects supervoxels 3 and 4
    +--------+--------+--------+
    |        |        |        |
    |   A    |   B    |   C    |
    |        |        |        |
    | 1+-+2+---+3=s=4+---+5+-+6|
    |        |        |        |
    +--------+--------+--------+
    """

    annotation_type = 'synapse'
    table_name = 'synapse'
    user_id = 'test_user'
    amdb, dataset_name = test_annon_dataset
    # success= amdb._delete_table(dataset_name, table_name)
    # assert(success)
    amdb.create_table(user_id, dataset_name, annotation_type, table_name)

    synapse_d = {
        "type": annotation_type,
        "pre_pt": {
            "position": [31, 0, 0],
        },
        "post_pt": {
            "position": [33, 0, 0],
        },
        "ctr_pt": {
            "position": [32, 0, 0]
        }
    }
    schema = get_type_schema(annotation_type)
    oids = import_annotation_func(
        synapse_d, user_id, dataset_name, table_name, schema, annotation_type, amdb)

    # bouton_shape_annotation = {
    #     'target_id': int(oids[0]),
    #     'shape': 'potato'
    # }

    # blank_sv_ids = np.array([], dtype=np.uint64)
    # ref_annotations = [(blank_sv_ids, json.dumps(bouton_shape_annotation))]
    # oids = amdb.insert_annotations(dataset_name=dataset_name,
    #                                annotation_type='bouton_shape',
    #                                annotations=ref_annotations,
    #                                user_id="user_id")
    # print('bouton_shape_oids', oids)
    yield test_annon_dataset


def test_simple_test(cv, test_data, test_annon_dataset, monkeypatch, requests_mock):

    amdb, dataset_name = test_annon_dataset

    class MyChunkedGraph(object):
        def __init__(a, **kwargs):
            pass

        def get_serialized_info(self):
            return {}

        def get_latest_roots(self, n_threads=1, time_stamp=None):
            return [k+1000 for k in range(2*2*2)]

        def get_root(self, atomic_id, time_stamp=None):
            supvox_coords = np.unravel_index(np.array(atomic_id),
                                             (4, 4, 4))
            root_coords = np.int64(np.array(supvox_coords)/2)
            root_id = np.ravel_multi_index(root_coords,
                                           (2, 2, 2))
            return root_id + 1000

    monkeypatch.setattr(materializationengine.materialize.chunkedgraph,
                        'ChunkedGraph',
                        MyChunkedGraph)

    def mocked_info(dataset):
        return cv, (1.0, 1.0, 1.0)

    monkeypatch.setattr(materializationengine.materialize,
                        'get_segmentation_and_scales_from_infoservice',
                        mocked_info)

    time_stamp = datetime.datetime.utcnow()
    df = materialize_all_annotations('cgtable',
                                     dataset_name=dataset_name,
                                     schema_name="synapse",
                                     table_name="synapse",
                                     version='v0',
                                     time_stamp=time_stamp,
                                     amdb_client=amdb.client,
                                     amdb_instance_id=amdb.instance_id,
                                     cg_instance_id='cgraph_instance',
                                     n_threads=1)
    df.to_csv('test.csv')
    

    df = materialize_root_ids('cgtable',
                              dataset_name=dataset_name,
                              time_stamp=time_stamp,
                              version='v0',
                              sqlalchemy_database_uri='postgres://postgres:synapsedb@localhost:5432/testing',
                              cg_instance_id='cgraph_instance',
                              n_threads=1)


    df = materialize_all_annotations('cgtable',
                                     dataset_name=dataset_name,
                                     schema_name="synapse",
                                     table_name="synapse",
                                     version='v0',
                                     amdb_client=amdb.client,
                                     amdb_instance_id=amdb.instance_id,
                                     sqlalchemy_database_uri='postgres://postgres:synapsedb@localhost:5432/testing',
                                     cg_instance_id='cgraph_instance',
                                     n_threads=1)
    
    # TODO do real tests of results
    # df_bs = materialize_all_annotations(table_id,
    #                                     dataset_name=dataset_name,
    #                                     annotation_type="bouton_shape",
    #                                     amdb_client=amdb.client,
    #                                     amdb_instance_id=amdb.instance_id,
    #                                     cg_client=cgraph.client,
    #                                     cg_instance_id=cgraph.instance_id,
    #                                     n_threads=1)

    # print(df_bs)
