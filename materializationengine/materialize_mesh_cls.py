from emannotationschemas.models import make_dataset_models, Base, format_table_name
from emannotationschemas.mesh_models import make_neuron_compartment_model
from emannotationschemas.mesh_models import make_post_synaptic_compartment_model
from geoalchemy2.shape import to_shape
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import time
import os
from meshparty import trimesh_io
from multiwrapper import multiprocessing_utils as mu
import zlib
import numpy as np
from scipy import sparse, spatial
from datajoint.blob import unpack


HOME = os.path.expanduser("~")

def wkb_to_numpy(wkb):
    shp = to_shape(wkb)
    return np.array([shp.xy[0][0], shp.xy[1][0], shp.z], dtype=np.int)


def extract_compartment_labels(cm_args):
    cm_labels = unpack(cm_args[1])
    cm_vertices = unpack(cm_args[2])
    root_id = cm_args[0]

    return (cm_labels, cm_vertices, root_id)

def read_mesh(mesh_dir, root_id):
    mesh_path = os.path.join(mesh_dir, "{}.h5".format(root_id))
    meshmeta = trimesh_io.MeshMeta()
    mesh = meshmeta.mesh(mesh_path)
    return mesh

def get_synapse_vertices(synapses, kdtree, voxel_size=[4, 4, 40]):
    poss = []
    for synapse in synapses:
        poss.append(wkb_to_numpy(synapse.ctr_pt_position)* voxel_size)

    _, near_vertex_ids = kdtree.query(poss, n_jobs=1)
    return near_vertex_ids

def add_synapses_to_session(synapses, dm_labelled, cm_labels, session,
                            PostSynapseCompartment, max_distance =15):
     # for each of the synapse vertices, find the closest labeled vertex
    closest_dm_labelled = np.argmin(dm_labelled, axis=1)

    # find what the minimum distance is
    min_d = np.min(dm_labelled, axis=1)

    # loop over synapses
    for k, synapse in enumerate(synapses):
        # if the closest mesh point is within 15 hops, use the found label
        if min_d[k] < max_distance:
            # need to index into the list of closest indices, and then into the labels
            label = cm_labels[closest_dm_labelled[k]]
            # initialize a new label
            psc = PostSynapseCompartment(label=int(label), synapse_id=synapse.id)
            # add it to database session
            session.add(psc)

def associate_synapses_single(args):
    # script parameters
    DATABASE_URI = "postgresql://postgres:welcometothematrix@35.196.105.34/postgres"
    dataset = 'pinky100'
    synapse_table = 'pni_synapses_i2'
    version = 36
    mesh_dir = '{}/meshes/'.format(HOME)
    voxel_size = [4,4,40] # how to convert from synpase voxel index to mesh units (nm)
    engine = create_engine(DATABASE_URI, echo=False)

    # assures that all the tables are created
    # would be done as a db management task in general
    Base.metadata.create_all(engine)

    # create a session class
    # this will produce session objects to manage a single transaction
    Session = sessionmaker(bind=engine)
    session = Session()

    # build the cell segment and synapse models for this version
    model_dict = make_dataset_models(dataset,
                                     [('synapse', synapse_table)],
                                     version=version)
    SynapseModel = model_dict[synapse_table]

    # build the compartment models and post synaptic compartment models
    PostSynapseCompartment = make_post_synaptic_compartment_model(dataset,
                                                                  synapse_table,
                                                                  version=version)

    time_start = time.time()

    # extract the vertices labels and root_id from database model
    cm_labels, cm_vertices, root_id = extract_compartment_labels(args)

    # read the mesh from h5 file on disk using meshparty
    mesh = read_mesh(mesh_dir, root_id)

    print(root_id)

    print("%d - loading = %.2fs" % (root_id, time.time() - time_start))
    time_start = time.time()

    # build a kd tree of mesh vertices
    kdtree = spatial.cKDTree(mesh.vertices)
    print("%d - kdtree = %.2fs" % (root_id, time.time() - time_start))

    time_start = time.time()
    # query the kdtree to find the index of the closest mesh vertex to our labelled vertices
    (labeled_ds, labeled_vertices) = kdtree.query(cm_vertices, n_jobs=1)
    print("%d - query = %.2fs" % (root_id, time.time() - time_start))
    time_start = time.time()

    # TODO add checking for large distances to filter out irrelavent labels,
    # potential speed up to remove this step if cm_vertices are predictable indices

    # get all the synapses onto this rootId from database
    synapses = session.query(SynapseModel).filter(
        SynapseModel.post_pt_root_id == root_id).all()

    # find the index of the closest mesh vertex
    synapse_vertices = get_synapse_vertices(synapses, kdtree,
                                            voxel_size=voxel_size)

    print("%d - spatial lookup = %.2fs" % (root_id, time.time() - time_start))
    time_start = time.time()

    block_size = 100
    for i_synapse_block in range(0, len(synapse_vertices), block_size):

        synapse_block = synapses[i_synapse_block: i_synapse_block + block_size]
        synapse_vertex_block = synapse_vertices[i_synapse_block: i_synapse_block + block_size]

        # calculate the distance from the synapse vertices to mesh points within 15 edges
        dm = sparse.csgraph.dijkstra(mesh.csgraph, indices=synapse_vertex_block,
                                     directed=True, limit=15)
        time_start = time.time()


        # only consider the mesh vertices for which we have labels
        dm_labelled = dm[:, labeled_vertices]

        # for each of the synapse vertices, find the closest labeled vertex
        # closest_dm_labelled = np.argmin(dm_labelled, axis=1)

        # find what the minimum distance is
        # min_d = np.min(dm_labelled, axis=1)

        # add the labels to the session
        add_synapses_to_session(synapse_block, dm_labelled, cm_labels, session,
                                PostSynapseCompartment)

        print("%d - %d / %d - dijkstra and adding to session = %.2fs" % (root_id, i_synapse_block, len(synapse_vertices), time.time() - time_start))

    # commit all synapse labels to database
    session.commit()
    print("%d - commit = %.2fs" % (root_id, time.time() - time_start))