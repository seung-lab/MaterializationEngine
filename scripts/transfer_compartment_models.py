import argschema
import os
import marshmallow as mm
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from emannotationschemas import mesh_models, models

HOME = os.path.expanduser("~")


class BatchMaterializationSchema(argschema.ArgSchema):
    dataset_name = mm.fields.Str(default="pinky100",
                                 required=True,
                                 description="name of dataset in DynamicAnnotationDb")
    source_version = mm.fields.Int(required=True,
                                   description="version number to copy labels from")
    target_version = mm.fields.Int(required=True,
                                   description="version number to copy labels to")
    sql_uri = mm.fields.Str(
        description="connection uri string to postgres database"
        "(default to MATERIALIZATION_POSTGRES_URI environment variable")


if __name__ == '__main__':
    mod = argschema.ArgSchemaParser(schema_type=BatchMaterializationSchema)
    if 'sql_uri' not in mod.args:
        if 'MATERIALIZATION_POSTGRES_URI' in os.environ.keys():
            sql_uri = os.environ['MATERIALIZATION_POSTGRES_URI']
        else:
            raise Exception(
                'need to define a postgres uri via command line or MATERIALIZATION_POSTGRES_URI env')
    else:
        sql_uri = mod.args['sql_uri']
    source_version = mod.args['source_version']
    target_version = mod.args['target_version']
    dataset_name = mod.args['dataset_name']
    RootModelFrom = models.make_cell_segment_model(
        dataset_name, version=source_version)
    RootModelTo = models.make_cell_segment_model(
        dataset_name, version=target_version)
          
    CompartmentModelFrom = mesh_models.make_neuron_compartment_model(dataset_name,
                                                                     version=source_version)

    CompartmentModelTo = mesh_models.make_neuron_compartment_model(dataset_name,
                                                                   version=target_version)
    
    engine = create_engine(sql_uri)
    models.Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    unchanged_query = session.query(CompartmentModelFrom).join(RootModelTo, RootModelTo.id==CompartmentModelFrom.root_id)
    num_models_from = session.query(CompartmentModelFrom).count()

    overlapped_models = unchanged_query.all()
    
    for model in overlapped_models:
        cmt = CompartmentModelTo(root_id = model.root_id,
                                 vertices = model.vertices,
                                 labels=model.labels)
        session.add(cmt)
    session.commit()

