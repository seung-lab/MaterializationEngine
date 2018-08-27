import numpy as np
from math import inf
import json
import pytest
from materializationengine.materialize import materialize_all_annotations
from materializationengine.materialize import materialize_root_ids
import datetime
from emannotationschemas.models import make_dataset_models, root_model_name
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def create_chunk(cgraph, vertices=None, edges=None, timestamp=None):
    """
    Helper function to add vertices and edges to the chunkedgraph - no safety checks!
    """
    if not vertices:
        vertices = []

    if not edges:
        edges = []

    vertices = np.unique(np.array(vertices, dtype=np.uint64))
    edges = [(np.uint64(v1), np.uint64(v2), np.float32(aff))
             for v1, v2, aff in edges]
    edge_ids = []
    cross_edge_ids = []
    edge_affs = []
    cross_edge_affs = []
    isolated_node_ids = [x for x in vertices if (x not in [edges[i][0] for i in range(len(edges))]) and
                                                (x not in [edges[i][1] for i in range(len(edges))])]

    for e in edges:
        if cgraph.test_if_nodes_are_in_same_chunk(e[0:2]):
            edge_ids.append([e[0], e[1]])
            edge_affs.append(e[2])
        else:
            cross_edge_ids.append([e[0], e[1]])
            cross_edge_affs.append(e[2])

    edge_ids = np.array(edge_ids, dtype=np.uint64).reshape(-1, 2)
    edge_affs = np.array(edge_affs, dtype=np.float32).reshape(-1, 1)
    cross_edge_ids = np.array(cross_edge_ids, dtype=np.uint64).reshape(-1, 2)
    cross_edge_affs = np.array(
        cross_edge_affs, dtype=np.float32).reshape(-1, 1)
    isolated_node_ids = np.array(isolated_node_ids, dtype=np.uint64)

    cgraph.add_atomic_edges_in_chunks(edge_ids, cross_edge_ids,
                                      edge_affs, cross_edge_affs,
                                      isolated_node_ids)


def to_label(cgraph, l, x, y, z, segment_id):
    return cgraph.get_node_id(np.uint64(segment_id), layer=l, x=x, y=y, z=z)


@pytest.fixture(scope="session")
def test_data(chunkgraph_tuple, test_annon_dataset):
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
    amdb, dataset_name = test_annon_dataset
    cgraph, table_id = chunkgraph_tuple

    # Chunk A
    create_chunk(cgraph,
                 vertices=[to_label(cgraph, 1, 0, 0, 0, 0),
                           to_label(cgraph, 1, 0, 0, 0, 1)],
                 edges=[(to_label(cgraph, 1, 0, 0, 0, 0),
                         to_label(cgraph, 1, 0, 0, 0, 1), 0.5),
                        (to_label(cgraph, 1, 0, 0, 0, 1),
                         to_label(cgraph, 1, 1, 0, 0, 0), inf)])

    # Chunk B
    create_chunk(cgraph,
                 vertices=[to_label(cgraph, 1, 1, 0, 0, 0),
                           to_label(cgraph, 1, 1, 0, 0, 1)],
                 edges=[(to_label(cgraph, 1, 1, 0, 0, 0),
                         to_label(cgraph, 1, 0, 0, 0, 0), inf),
                        (to_label(cgraph, 1, 1, 0, 0, 1),
                         to_label(cgraph, 1, 2, 0, 0, 0), inf)])

    # Chunk C
    create_chunk(cgraph,
                 vertices=[to_label(cgraph, 1, 2, 0, 0, 0),
                           to_label(cgraph, 1, 2, 0, 0, 1)],
                 edges=[(to_label(cgraph, 1, 2, 0, 0, 0),
                         to_label(cgraph, 1, 2, 0, 0, 1), 0.5),
                        (to_label(cgraph, 1, 2, 0, 0, 0),
                         to_label(cgraph, 1, 1, 0, 0, 1), inf)])

    cgraph.add_layer(3, np.array([[0, 0, 0], [1, 0, 0]]))
    cgraph.add_layer(3, np.array([[2, 0, 0]]))

    cgraph.add_layer(4, np.array([[0, 0, 0], [1, 0, 0]]))

    pre_id = to_label(cgraph, 1, 1, 0, 0, 0)
    post_id = to_label(cgraph, 1, 1, 0, 0, 1)
    synapse_d = {
        "type": annotation_type,
        "pre_pt": {
            "position": [1000, 256, 8],
            "supervoxel_id": int(pre_id)
        },
        "post_pt": {
            "position": [1010, 256, 8],
            "supervoxel_id": int(post_id)
        },
        "ctr_pt": {
            "position": [1005, 256, 8]
        }
    }

    sv_ids = np.array([pre_id, post_id], dtype=np.uint64)
    annotations = [(sv_ids, json.dumps(synapse_d))]

    # Insert into table
    oids = amdb.insert_annotations(dataset_name=dataset_name,
                                   annotation_type=annotation_type,
                                   annotations=annotations,
                                   user_id="user_id")

    print('oids', oids)
    yield chunkgraph_tuple


def test_simple_test(test_data, test_annon_dataset, dburi):
    cgraph, table_id = test_data
    amdb, dataset_name = test_annon_dataset

    time_stamp = datetime.datetime.utcnow()

    root_d = materialize_root_ids(table_id,
                                  dataset_name,
                                  time_stamp,
                                  cg_client=cgraph.client,
                                  cg_instance_id=cgraph.instance_id,
                                  sqlalchemy_database_uri=dburi,
                                  n_threads=1)

    syn_df = materialize_all_annotations(table_id,
                                         dataset_name=dataset_name,
                                         annotation_type="synapse",
                                         time_stamp=time_stamp,
                                         sqlalchemy_database_uri=dburi,
                                         amdb_client=amdb.client,
                                         amdb_instance_id=amdb.instance_id,
                                         cg_client=cgraph.client,
                                         cg_instance_id=cgraph.instance_id,
                                         n_threads=1)

    if dburi is None:
        assert(len(root_d.keys()) == 2)
        assert(syn_df.shape[0] == 1)
    else:
        models = make_dataset_models(dataset_name)
        RootModel = models[root_model_name.lower()]
        SynapseModel = models['synapse']
        engine = create_engine(dburi,
                               echo=False, pool_size=20,
                               max_overflow=-1)

        Session = sessionmaker(bind=engine)
        session = Session()
        num_roots = session.query(RootModel).count()
        assert(num_roots == 2)
        num_synapses = session.query(SynapseModel).count()
        assert(num_synapses == 1)