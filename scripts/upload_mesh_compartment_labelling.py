from emannotationschemas.models import make_dataset_models, Base
from emannotationschemas.mesh_models import make_neuron_compartment_model
from emannotationschemas.base import flatten_dict
from emannotationschemas import get_schema
from geoalchemy2.shape import to_shape
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import time
import pandas as pd
import os
import h5py
from meshparty import trimesh_io
import zlib
import numpy as np
from datajoint.blob import pack

# example of initializing mapping of database
DATABASE_URI = "postgresql://postgres:welcometothematrix@35.196.105.34/postgres"
dataset = 'pinky100'
version = 36
subsampling = 10
synapse_table = 'pni_synapses_i2'
engine = create_engine(DATABASE_URI, echo=False)
model_dict = make_dataset_models(dataset,
                                 [('synapse', synapse_table)],
                                 version=version)
CompartmentModel = make_neuron_compartment_model(dataset, version=version)

# assures that all the tables are created
# would be done as a db management task in general
Base.metadata.create_all(engine)

# create a session class
# this will produce session objects to manage a single transaction
Session = sessionmaker(bind=engine)
session = Session()


mesh_class_dir = 'mesh_cls/pinky40_full_ae_750_local_nonorm_nobn_v12'

mesh_dir = './meshes'
files=[f for f in os.listdir(mesh_class_dir) if f.endswith('.h5')]
seg_ids = [int(os.path.splitext(f)[0]) for f in files]

for filename, seg_id in zip(files, seg_ids):

    filepath = os.path.join(mesh_class_dir, filename)

    f = h5py.File(filepath, 'r')
    # mesh_endpoint = "https://www.dynamicannotationframework.com/meshing/"
    # cv_path = "https://storage.googleapis.com/neuroglancer/nkem/pinky100_v0/ws/lost_no-random/bbox1_0"

    # print(seg_ids)
    # trimesh_io.download_meshes(seg_ids=seg_ids,
    #                            target_dir=mesh_dir,
    #                            cv_path=cv_path, 
    #                            fmt="obj",
    #                            mesh_endpoint=mesh_endpoint,
    #                            n_threads=15,
    #                            overwrite=False)
    meshmeta = trimesh_io.MeshMeta()
    meshpath = os.path.join(mesh_dir,'{}.h5'.format(seg_id))
    mesh = meshmeta.mesh(meshpath)
    pred = f['pred']
    pred_subsample=pred[0::subsampling]
    vertices_subsample=mesh.vertices[0::subsampling,:]

    cm = CompartmentModel(vertices=pack(vertices_subsample),
                          labels=pack(np.uint8(pred_subsample)),
                          root_id=seg_id)

    session.add(cm)
    session.commit()

