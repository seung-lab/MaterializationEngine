import json
import functools
from emannotationschemas import models as em_models
from emannotationschemas import get_schema
from emannotationschemas.models import root_model_name
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import numpy as np


class MaterializeAnnotationException(Exception):
    pass


class RootIDNotFoundException(MaterializeAnnotationException):
    pass


class AnnotationParseFailure(MaterializeAnnotationException):
    pass

def create_all_models(amdb, dataset):
    metadata_list = amdb.get_existing_tables_metadata(dataset)
    schema_tables = [(md['schema_name'], md['table_name']) for md in metadata_list]
    mdict = {md['table_name']: md for md in metadata_list}
    models = em_models.make_dataset_models(dataset,
                                         schema_tables,
                                         metadata_dict=mdict)
    return models

def create_new_version(sql_uri, dataset, time_stamp):
    engine = create_engine(sql_uri)
    Session = sessionmaker(bind=engine)
    session = Session()

    top_version = (session.query(em_models.AnalysisVersion)
                   .order_by(em_models.AnalysisVersion.version.desc())
                   .first())

    if top_version is None:
        new_version_number = 1
    else:
        new_version_number = top_version.version + 1

    analysisversion = em_models.AnalysisVersion(dataset=dataset,
                                                time_stamp=time_stamp,
                                                version=new_version_number)
    session.add(analysisversion)
    session.commit()
    return analysisversion


def lookup_sv_and_cg_bsp(cg,
                         cv,
                         item,
                         pixel_ratios=(1.0, 1.0, 1.0),
                         time_stamp=None):
    """ function to fill in the supervoxel_id and root_id

    :param cg: pychunkedgraph.ChunkedGraph
        chunkedgraph instance to use to lookup supervoxel and root_ids
    :param item: dict
        deserialized boundspatialpoint to process
    :param pixel_ratios: tuple
        ratios to multiple position coordinates
        to get cg.cv segmentation coordinates
    :param time_stamp: datetime.datetime
        time_stamp to lock to (optional None)
    :return: None
        will edit item in place
    """
    if time_stamp is None:
        raise AnnotationParseFailure('passing no timestamp')

    try:
        voxel = np.array(item['position'])*np.array(pixel_ratios)
        sv_id = cv[int(voxel[0]), int(voxel[1]), int(voxel[2])].flatten()[0]
    except Exception as e:
        msg = "failed to lookup sv_id of voxel {}. reason {}".format(voxel, e)
        raise AnnotationParseFailure(msg)

    if sv_id == 0:
        root_id = 0
    else:
        try:
            root_id = cg.get_root(sv_id, time_stamp=time_stamp)
        except Exception as e:
            msg = "failed to lookup root_id of sv_id {} {}".format(sv_id, e)
            raise AnnotationParseFailure(msg)

    item['supervoxel_id'] = int(sv_id)
    item[root_model_name.lower()+"_id"] = int(root_id)


class MaterializationManager(object):
    def __init__(self, dataset_name, schema_name,
                 table_name, version = 1, version_id = None,
                 annotation_model=None,
                 sqlalchemy_database_uri=None):
        self._dataset_name = dataset_name
        self._schema_name = schema_name
        self._table_name = table_name
        self._version = version
        self._version_id = version_id
        self._sqlalchemy_database_uri = sqlalchemy_database_uri

        em_models.make_dataset_models(dataset_name, [])

        if annotation_model is not None:
            self._annotation_model, self._annotation_submodel_d = annotation_model
        else:
            self._annotation_model, self._annotation_submodel_d = em_models.make_annotation_model(
                dataset_name, schema_name, table_name)

        if sqlalchemy_database_uri is not None:
            self._sqlalchemy_engine = create_engine(sqlalchemy_database_uri,
                                                    echo=True)
            em_models.Base.metadata.create_all(self.sqlalchemy_engine)

            self._sqlalchemy_session = sessionmaker(
                bind=self.sqlalchemy_engine)
        else:
            self._sqlalchemy_engine = None
            self._sqlalchemy_session = None

        try:
            self._schema_init = get_schema(self.schema_name)
        except:
            self._schema_init = None

        self._this_sqlalchemy_session = None

    @property
    def schema_name(self):
        return self._schema_name

    @property
    def table_name(self):
        return self._table_name

    @property
    def dataset_name(self):
        return self._dataset_name

    @property
    def version(self):
        return self._version

    @property
    def sqlalchemy_database_uri(self):
        return self._sqlalchemy_database_uri

    @property
    def sqlalchemy_engine(self):
        return self._sqlalchemy_engine

    @property
    def sqlalchemy_session(self):
        return self._sqlalchemy_session

    @property
    def this_sqlalchemy_session(self):
        if self._this_sqlalchemy_session is None:
            self._this_sqlalchemy_session = self.sqlalchemy_session()
        return self._this_sqlalchemy_session

    @property
    def annotation_model(self):
        return self._annotation_model

    @property
    def is_sql(self):
        return not self.sqlalchemy_database_uri is None

    @property
    def schema_init(self):
        return self._schema_init

    def get_serialized_info(self):
        """ Puts all initialization parameters into a dict

        :return: dict
        """
        info = {"dataset_name": self.dataset_name,
                "schema_name": self.schema_name,
                "table_name": self.table_name,
                "version": self.version,
                "sqlalchemy_database_uri": self.sqlalchemy_database_uri}
        print(info)
        return info

    def _drop_table(self):
        """ Deletes the table in the database """
        table_name = em_models.format_table_name(self._dataset_name,
                                                 self._table_name,
                                                 self._version)
        if table_name in em_models.Base.metadata.tables:
            em_models.Base.metadata.tables[table_name].drop(self.sqlalchemy_engine)
        else:
            print('could not drop {}'.format(table_name))

    def add_to_analysis_table(self):
        """ Adds annotation info to database """
        assert self.is_sql
        assert self._version_id is not None

        at = em_models.AnalysisTable(table_name=self.table_name,
                                     schema_name=self.schema_name,
                                     analysisversion_id=self._version_id)
        self.this_sqlalchemy_session.add(at)
        self.commit_session()

    def get_schema(self, cg, cv, pixel_ratios=(1.0, 1.0, 1.0), time_stamp=None):
        """ Loads schema with appropriate context

        :param cg: pychunkedgraph.ChunkedGraph
            chunkedgraph instance to lookup root_ids from atomic_ids
        :param cv: cloudvolume.CloudVolume
            cloudvolue instanceto look_up atomic_ids
        :param pixel_ratios: tuple
            ratio of position units to units of cv
        :return:
        """
        context = dict()
        context['bsp_fn'] = functools.partial(lookup_sv_and_cg_bsp,
                                              cg, cv, pixel_ratios=pixel_ratios,
                                              time_stamp=time_stamp)
        context['postgis'] = self.is_sql
        return self.schema_init(context=context)

    def flatten_annotation_into_submodels(self, data, id_, version):
        data[self.table_name+'_id'] = id_
        data['version'] = version
        for key, value in data.items():
            if type(value) == dict:
                value['version'] = version
                value[self.table_name+'_id'] = id_
        return data

    def deserialize_single_annotation(self, blob, cg, cv,
                                      pixel_ratios=(1.0, 1.0, 1.0),
                                      time_stamp=None):
        """ Materializes single annotation object

        :param blob: binary
        :param sv_id_to_root_id_dict: dict
        :param pixel_ratios: tuple
            length 3 tuple of ratios to multiple position
            coordinates to get to segmentation coordinates
        :param time_stamp: datetime.datetime
            time_stamp to lock database to
        :return: dict
        """
        schema = self.get_schema(
            cg, cv, pixel_ratios=pixel_ratios, time_stamp=time_stamp)
        data = schema.load(json.loads(blob)).data
 
        return data

    def bulk_insert_annotations(self, annotations):
        assert self.is_sql

        insert_list = []
        if self._annotation_submodel_d is not None:
            for field, sub_model in self._annotation_submodel_d.items():
                sub_annotations = [a.pop(field) for a in annotations]
                insert_list.append((sub_model, sub_annotations))
        self.this_sqlalchemy_session.bulk_insert_mappings(self.annotation_model,
                                                          annotations)
        for model, sub_annotations in insert_list:
            self.this_sqlalchemy_session.bulk_insert_mappings(model, sub_annotations)
                                                          

        

    def add_annotation_to_sql_database(self, deserialized_annotation):
        """ Transforms annotation object into postgis format and commits to the
            database

        :param deserialized_annotation: dict
        """
        assert self.is_sql

        # remove the type field because we don't want it as a column
        deserialized_annotation.pop('type', None)

        # # create a new model instance with data
        annotation = self.annotation_model(**deserialized_annotation)

        # add this annotation object to database
        self.this_sqlalchemy_session.add(annotation)

    def commit_session(self):
        # commit this transaction to database
        self.this_sqlalchemy_session.commit()

        self._this_sqlalchemy_session = None
