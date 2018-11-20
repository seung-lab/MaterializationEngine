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
from scipy.spatial import cKDTree
from scipy.sparse.csgraph import dijkstra
from datajoint.blob import unpack

HOME = os.path.expanduser("~")

from materializationengine import materialize_mesh_cls

if __name__ == '__main__':
    # script parameters
    DATABASE_URI = "postgresql://postgres:welcometothematrix@35.196.105.34/postgres"
    dataset = 'pinky100'
    synapse_table = 'pni_synapses_i2'
    version = 36
    mesh_dir = '{}/meshes/'.format(HOME)
    voxel_size = [4,4,40] # how to convert from synpase voxel index to mesh units (nm)
    engine = create_engine(DATABASE_URI, echo=False)


    # build the compartment models and post synaptic compartment models
    CompartmentModel = make_neuron_compartment_model(dataset,
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

    multi_args = []
    for cm in cms:
        multi_args.append([cm.root_id, cm.labels, cm.vertices])

    mu.multisubprocess_func(materialize_mesh_cls.associate_synapses_single,
                            multi_args, n_threads=64,
                            package_name="materializationengine")

    # for cm in cms:
    #     print(cm)
    #     associate_synapses_single(cm)
