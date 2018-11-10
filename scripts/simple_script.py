from emannotationschemas.models import AnalysisVersion, AnalysisTable
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
import requests

sql_uri = "postgres://postgres:synapsedb@localhost:5432/testing"
engine = create_engine(sql_uri)
Session = sessionmaker(bind=engine)
session = Session()

# from emannotationschemas.models import make_annotation_model, get_next_version
# from materializationengine.database import Base
#from emannotationschemas.models import Base as EMSBase
# sql_uri = 'postgres://postgres:welcometothematrix@35.196.105.34:5432/postgres'

dataset = "test_dataset"
version = 8

# annotation_service_base = "https://www.dynamicannotationframework.com/annotation/"
# url = annotation_service_base + "dataset/{}".format(dataset)
# print(url)
# tables = requests.get(url).json()
# print(tables)

version = session.query(AnalysisVersion).first()
print(version)

#version = (session.query(AnalysisVersion)
#            .filter(AnalysisVersion.version == version)
#            .filter(AnalysisVersion.dataset == dataset).first())
# version = AnalysisVersion()

# print(version)
# for table in tables:
#     at= AnalysisTable(tablename=table['table_name'],
#                       schema=table['schema_name'],
#                       analysisversion=version)
#     session.add(at)
# session.commit()

# # version_tables = [t for t in tables if (t.startswith(dataset) & t.endswith("v{}".format(version)))]
# table_name = version_tables[0]
# table_name.split(dataset)[1][1:]
# print(version_tables)
# os.environ['BIGTABLE_EMULATOR_HOST']='localhost:8086'
# amdb = AnnotationMetaDB(instance_id='test')
# md=amdb.get_table_metadata(dataset, version_tables[0])
# print(md)
# session = sessionmaker(bind=engine)()
# make_dataset_models(dataset, [], version=version)

# SynapseModel = make_annotation_model(dataset,
#                                      'synapse',
#                                      'synapse',
#                                      version=version)


# soma_valence = session.query(SynapseModel).all()
# print(soma_valence)