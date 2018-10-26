import numpy as np
from math import inf
import json
import pytest

from pychunkedgraph.backend import chunkedgraph

from materializationengine.materialize import materialize_all_annotations
from annotationengine.annotation import collect_bound_spatial_points, import_annotation_func, get_schema_from_service
from emannotationschemas.blueprint_app import get_type_schema
from mock import patch, Mock, MagicMock


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
    amdb.create_table(user_id, dataset_name, annotation_type, table_name)

    synapse_d = {
        "type": annotation_type,
        "pre_pt": {
            "position": [1000, 256, 8],
        },
        "post_pt": {
            "position": [1010, 256, 8],
        },
        "ctr_pt": {
            "position": [1005, 256, 8]
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


def simple_get_root(self, atomic_id):
    if (atomic_id <= 3):
        return "12341324131851234"
    else:
        return "1345813419234993222"


def test_simple_test(test_annon_dataset, monkeypatch):

    amdb, dataset_name = test_annon_dataset

    print(amdb.get_existing_tables())

    monkeypatch.setattr('pychunkedgraph.backend.chunkedgraph.ChunkedGraph',
                        MagicMock(chunkedgraph.ChunkedGraph))
    monkeypatch.setattr(
        'pychunkedgraph.backend.chunkedgraph.ChunkedGraph.get_root', simple_get_root)

    df = materialize_all_annotations('cgtable',
                                     dataset_name=dataset_name,
                                     annotation_type="synapse",
                                     table_name = "synapse",
                                     version='v1',
                                     amdb_client=amdb.client,
                                     amdb_instance_id=amdb.instance_id,
                                     cg_instance_id='cgraph_instance',
                                     n_threads=1)
    print(df)

    # df_bs = materialize_all_annotations(table_id,
    #                                     dataset_name=dataset_name,
    #                                     annotation_type="bouton_shape",
    #                                     amdb_client=amdb.client,
    #                                     amdb_instance_id=amdb.instance_id,
    #                                     cg_client=cgraph.client,
    #                                     cg_instance_id=cgraph.instance_id,
    #                                     n_threads=1)

    # print(df_bs)
