from emannotationschemas.models import make_dataset_models, Base, format_table_name
from emannotationschemas.mesh_models import make_neuron_compartment_model
from emannotationschemas.mesh_models import make_post_synaptic_compartment_model
from geoalchemy2.shape import to_shape
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import time
import os
from meshparty import trimesh_io
import zlib
import numpy as np
from scipy.spatial import KDTree
from scipy.sparse.csgraph import dijkstra
from datajoint.blob import unpack

def wkb_to_numpy(wkb):
    shp = to_shape(wkb)
    return np.array([shp.xy[0][0], shp.xy[1][0], shp.z], dtype=np.int)


def extract_compartment_labels(cm):
    cm_labels = unpack(cm.labels)
    cm_vertices = unpack(cm.vertices)
    root_id = cm.root_id
    return (cm_labels, cm_vertices, root_id)

def read_mesh(mesh_dir, root_id):
    mesh_path = os.path.join(mesh_dir, "{}.h5".format(root_id))
    meshmeta = trimesh_io.MeshMeta()
    mesh = meshmeta.mesh(mesh_path)
    return mesh

def get_synapse_vertices(synapses, kdtree, voxel_size=[4, 4, 40]):
    synapse_vertices = []
    for synapse in synapses:
        pos = wkb_to_numpy(synapse.ctr_pt_position)
        pos_nm = pos * voxel_size
        (d, near_vertex) = kdtree.query(pos_nm)
        synapse_vertices.append(near_vertex)
    return synapse_vertices

def add_synapses_to_session(synapses, dm_labelled, cm_labels, session, max_distance =15 ):
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

# def get_closest_labels(labels,
#                        labeled_vertices,
#                        query_vertices,
#                        mesh,
#                        max_search_depth = 15,
#                        kdtree=None,
#                        max_search_radius = 500):
    
#     # calculate the distance from the synapse vertices to mesh points within 15 edges
#     dm = dijkstra(mesh.edges_sparse, False, query_vertices, False, limit=max_search_depth)

#     # only consider the mesh vertices for which we have labels
#     dm_labelled = dm[:, labeled_vertices]
    
#     # for each of the synapse vertices, find the closest labeled vertex
#     closest_dm_labelled = np.argmin(dm_labelled, axis=1)

#     # find what the minimum distance is
#     min_d = np.min(dm_labelled, axis=1)

#     if np.sum(min_d>max_search_depth)>1:
#         if kdtree is not None:
#             bad_synapses = query_vertices[min_d<max_search_depth]
#             distances, vertices = kdtree.query(bad_synapses, k=100, distance_upper_bound=max_search_radius)

#         else:
#     else:
#         labels = 


# script parameters
DATABASE_URI = "postgresql://postgres:welcometothematrix@35.196.105.34/postgres"
dataset = 'pinky100'
synapse_table = 'pni_synapses_i2'
version = 36
mesh_dir = './meshes' # where on disk you have already downloaded the meshes
voxel_size = [4,4,40] # how to convert from synpase voxel index to mesh units (nm)
engine = create_engine(DATABASE_URI, echo=False)

# build the cell segment and synapse models for this version
model_dict = make_dataset_models(dataset,
                                 [('synapse', synapse_table)],
                                 version=version)
SynapseModel = model_dict[synapse_table]

# build the compartment models and post synaptic compartment models
CompartmentModel = make_neuron_compartment_model(dataset,
                                                 version=version)
PostSynapseCompartment = make_post_synaptic_compartment_model(dataset,
                                                              synapse_table,
                                                              version=version)

# assures that all the tables are created
# would be done as a db management task in general
Base.metadata.create_all(engine)

# create a session class
# this will produce session objects to manage a single transaction
Session = sessionmaker(bind=engine)
session = Session()

# query the database for all the compartment models that are available
cms = session.query(CompartmentModel).all()
timings = {}

time_start = time.time()

for cm in cms:
    time_start = time.time()
    
    # extract the vertices labels and root_id from database model
    cm_labels, cm_vertices, root_id = extract_compartment_labels(cm)

    #read the mesh from h5 file on disk using meshparty
    mesh = read_mesh(mesh_dir, root_id)
    
    time_start_2 = time.time()
    #build a kd tree of mesh vertices
    kdtree = KDTree(mesh.vertices)
    timings["{} kdtree".format(cm.root_id)]=time.time()-time_start_2

    time_start_2 = time.time()
    # query the kdtree to find the index of the closest mesh vertex to our labelled vertices
    (labeled_ds, labeled_vertices) = kdtree.query(cm_vertices)
    timings["{} cm query".format(cm.root_id)]=time.time()-time_start_2
    # TODO add checking for large distances to filter out irrelavent labels,
    # potential speed up to remove this step if cm_vertices are predictable indices

    # get all the synapses onto this rootId from database
    synapses = session.query(SynapseModel).filter(
        SynapseModel.post_pt_root_id == root_id).all()

    # find the index of the closest mesh vertex
    synapse_vertices = get_synapse_vertices(synapses, kdtree, voxel_size=voxel_size)

    time_start_2 = time.time()
    # calculate the distance from the synapse vertices to mesh points within 15 edges
    dm = dijkstra(mesh.edges_sparse, False, synapse_vertices, False, limit=15)
    timings["{} dijkstra".format(cm.root_id)]=time.time()-time_start_2

    # only consider the mesh vertices for which we have labels
    dm_labelled = dm[:, labeled_vertices]
    
    # for each of the synapse vertices, find the closest labeled vertex
    closest_dm_labelled = np.argmin(dm_labelled, axis=1)

    # find what the minimum distance is
    min_d = np.min(dm_labelled, axis=1)

    # add the labels to the session
    add_synapses_to_session(synapses, dm_labelled, cm_labels, session)

    # commit all synapse labels to database
    session.commit()
    timings["{} all".format(cm.root_id)]=time.time()-time_start