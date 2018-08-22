import json
import functools

from emannotationschemas.models import make_annotation_model, Base
from emannotationschemas.base import flatten_dict
from emannotationschemas import get_schema
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import DropTable
from sqlalchemy.ext.compiler import compiles

@compiles(DropTable, "postgresql")
def _compile_drop_table(element, compiler, **kwargs):
    return compiler.visit_drop_table(element) + " CASCADE"

class MaterializeAnnotationException(Exception):
    pass


class RootIDNotFoundException(MaterializeAnnotationException):
    pass


class AnnotationParseFailure(MaterializeAnnotationException):
    pass


def materialize_bsp(sv_id_to_root_id_dict, item):
    """ Function to fill in root_id of BoundSpatialPoint fields
    using the dictionary provided. Will alter item in place.
    Meant to be passed to mm.Schema as a context variable ('bsp_fn')
    :param sv_id_to_root_id_dict: dict
        dictionary to look up root ids from supervoxel id
    :param item: dict
        deserialized boundspatialpoint to process
    :return: None
        will edit item in place
    """

    try:
        item['root_id'] = int(sv_id_to_root_id_dict[item['supervoxel_id']])
    except KeyError:
        msg = "cannot process {}, root_id not found in {}"
        raise RootIDNotFoundException(msg.format(item, sv_id_to_root_id_dict))


class DB(object):
    def __init__(self, sqlalchemy_database_uri):
        self._sqlalchemy_database_uri = sqlalchemy_database_uri

        self._sqlalchemy_engine = create_engine(sqlalchemy_database_uri,
                                                echo=False, pool_size=20,
                                                max_overflow=-1)
        Base.metadata.create_all(self.sqlalchemy_engine)

        self._sqlalchemy_session = sessionmaker(bind=self.sqlalchemy_engine)


    @property
    def sqlalchemy_database_uri(self):
        return self._sqlalchemy_database_uri

    @property
    def sqlalchemy_engine(self):
        return self._sqlalchemy_engine

    @property
    def sqlalchemy_session(self):
        return self._sqlalchemy_session



class MaterializationManager(object):
    def __init__(self, dataset_name, annotation_type, annotation_model=None,
                 sqlalchemy_database_uri=None):
        self._dataset_name = dataset_name
        self._annotation_type = annotation_type
        self._sqlalchemy_database_uri = sqlalchemy_database_uri

        if annotation_model is not None:
            self._annotation_model = annotation_model
        else:
            self._annotation_model = make_annotation_model(dataset_name,
                                                           annotation_type)

        if sqlalchemy_database_uri is not None:
            self._sqlalchemy_engine = create_engine(sqlalchemy_database_uri,
                                                    echo=False, pool_size=20,
                                                    max_overflow=-1)
            Base.metadata.create_all(self.sqlalchemy_engine)

            self._sqlalchemy_session = sessionmaker(bind=self.sqlalchemy_engine)
        else:
            self._sqlalchemy_engine = None
            self._sqlalchemy_session = None

        try:
            self._schema_init = get_schema(self.annotation_type)
        except:
            self._schema_init = None

    @property
    def annotation_type(self):
        return self._annotation_type

    @property
    def dataset_name(self):
        return self._dataset_name

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
                "annotation_type": self.annotation_type,
                "sqlalchemy_database_uri": self.sqlalchemy_database_uri}

        return info

    def _drop_table(self):
        """ Deletes the table in the database """
        table_name = "%s_%s" % (self.dataset_name, self.annotation_type)
        Base.metadata.tables[table_name].drop(self.sqlalchemy_engine)

    def get_schema(self, sv_id_to_root_id_dict):
        """ Loads schema with appropriate context

        :param root_id_d:
        :return:
        """
        assert self.schema_init is not None

        context = dict()
        context['bsp_fn'] = functools.partial(materialize_bsp,
                                              sv_id_to_root_id_dict)
        context['postgis'] = self.is_sql
        return self.schema_init(context=context)

    def deserialize_single_annotation(self, blob, sv_id_to_root_id_dict):
        """ Materializes single annotation object

        :param blob: binary
        :param sv_id_to_root_id_dict: dict
        :return: dict
        """

        schema = self.get_schema(sv_id_to_root_id_dict)
        data = schema.load(json.loads(blob)).data

        return flatten_dict(data)

    def add_annotation_to_sql_database(self, deserialized_annotation):
        self.add_annotations_to_sql_database([deserialized_annotation])

    def add_annotations_to_sql_database(self, deserialized_annotations):
        """ Transforms annotation object into postgis format and commits to the
            database

        :param deserialized_annotation: dict
        """
        assert self.is_sql

        # create a new db session
        this_session = self.sqlalchemy_session()

        for deserialized_annotation in deserialized_annotations:
            # remove the type field because we don't want it as a column
            deserialized_annotation.pop('type', None)

            # # create a new model instance with data
            annotation = self.annotation_model(**deserialized_annotation)

            # add this annotation object to database
            this_session.add(annotation)

        # commit this transaction to database
        this_session.commit()