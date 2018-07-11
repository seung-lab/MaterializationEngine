import numpy as np
from math import inf
import json


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
    rg2cg = dict(list(enumerate(vertices, 1)))
    cg2rg = {v: k for k, v in rg2cg.items()}

    cgraph.add_atomic_edges_in_chunks(edge_ids, cross_edge_ids,
                                      edge_affs, cross_edge_affs,
                                      isolated_node_ids, cg2rg, rg2cg)


def to_label(cgraph, l, x, y, z, segment_id):
    return cgraph.get_node_id(np.uint64(segment_id), layer=l, x=x, y=y, z=z)


def test_data(chunkgraph, annodb, test_annon_dataset):
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

    cgraph = chunkgraph

    # Chunk A
    create_chunk(cgraph,
                 vertices=[to_label(cgraph, 1, 0, 0, 0, 0),
                           to_label(cgraph, 1, 0, 0, 0, 1)],
                 edges=[(to_label(cgraph, 1, 0, 0, 0, 0),
                         to_label(cgraph, 1, 0, 0, 0, 1), 0.5),
                        (to_label(cgraph, 1, 0, 0, 0, 0),
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

    cgraph.add_layer(3, np.array([[0, 0, 0], [1, 0, 0], [2, 0, 0]]))

    res = cgraph.table.read_rows()
    res.consume_all()

    synapse_d = {
        "type": "synapse",
        "pre_pt": {
            "position": [1000, 256, 8],
            "supervoxel_id": 3
        },
        "post_pt": {
            "position": [1010, 256, 8],
            "supervoxel_id": 4
        },
        "ctr_pt": {
            "position": [1005, 256, 8]
        }
    }

    sv_ids = [np.array([3, 4], dtype=np.uint64)]
    annotations = [(sv_ids, json.dumps(synapse_d))]
    annodb.insert_annotations(test_annon_dataset, 'synapse', annotations,
                              "testing")
