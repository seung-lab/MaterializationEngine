from sqlalchemy.engine import reflection
from sqlalchemy import Index
from geoalchemy2.types import Geometry

class IndexCache:

    def __init__(self):
        self._index_maps = {}

    @property
    def index_maps(self):
        return self._index_maps

    @index_maps.setter
    def index_maps(self, empty_dict={}):
        self._index_maps = empty_dict  
    
    def get_table_indexes(self, table_name: str, engine):
        if table_name not in self._index_maps:
            insp = reflection.Inspector.from_engine(engine)
            try:
                pk_columns = insp.get_pk_constraint(table_name)
                indexed_columns = insp.get_indexes(table_name)
                foreign_keys = insp.get_foreign_keys(table_name)
            except Exception as e:
                print(f"No table named '{table_name}', error: {e}")
                return None
            index_map = {}
            if pk_columns:
                pk = {
                    'primary_key_name': pk_columns.get('name'),
                }
                if pk['primary_key_name']:
                    pk.update({
                        'column_name': pk_columns['constrained_columns'][0]
                    })
                    index_map.update({
                        'primary_key': pk
                    })
                else:
                    return None
            if indexed_columns:
                indexes = []
                for index in indexed_columns:
                    indx_map = {
                        'index_name': index.get("name"),
                        'column_name': index["column_names"][0],
                    }
                    indexes.append(indx_map.copy())

                index_map.update({
                    'indexes': indexes
                })
            if foreign_keys:
                for fk in foreign_keys:
                    fKeys = []
                    foreign_keys = {
                        'foreign_key': fk.get("name"),
                        'column_name': fk["constrained_columns"][0],
                    }
                    fKeys.append(foreign_keys.copy())
                index_map.update({
                    'foreign_keys': foreign_keys
                })
            self._index_maps[table_name] = index_map
        return self._index_maps[table_name]

    
    def get_index_from_model(self, model):
        model = model.__table__
        index_map = {}
        indexes = []
        for column in model.columns:    
            if column.primary_key:
                pk = {
                    'primary_key_name': f"{model.name}_pkey",
                    'column_name': column.name
                }        
                index_map.update({
                    'primary_key': pk
                })
            if column.index:
                indx_map = {
                    'index_name': f"ix_{model.name}_{column.name}",
                    'column_name': column.name
                }
                indexes.append(indx_map)
            if isinstance(column.type, Geometry): 
                spatial_index_map = {
                    'index_name': f"idx_{model.name}_{column.name}",
                    'column_name': column.name
                }
                indexes.append(spatial_index_map)        

        index_map.update({
            'indexes': indexes
        })

        self._index_maps[model.name] = index_map
        return self._index_maps[model.name]
    
    def drop_table_indexes(self, table_name: str, engine):
        indexes = self.get_table_indexes(table_name, engine)
        if not indexes:
            return f"No indexes on '{table_name}' found."
        command = f"ALTER TABLE {table_name}"
        for index_type, index_columns in indexes.items():
            if index_type == 'foreign_key':
                fk_list = [index_columns['foreign_key']
                           for i in index_columns]
                drop_fk = f"DROP CONSTRAINT {', '.join(fk_list)}"
                command = f"{command} {drop_fk}"
            if index_type == 'primary_key':
                drop_pk = f"DROP CONSTRAINT {index_columns['primary_key_name']}"
                command = f"{command} {drop_pk};"
            if index_type == 'indexes':
                index_list = [col['index_name'] for col in index_columns]
                drop_index = f"DROP INDEX {', '.join(index_list)}"
                command = f"{command} {drop_index}"
        try:
            connection = engine.connect()
            connection.execute(f"{command}")
        except Exception as e:
            raise(e)
        return True

    def add_indexes(self, table_name, model, engine, is_flat=True):
        if table_name not in self._index_maps:
            indexes = self.get_index_from_model(model)
        else:
            indexes = self._index_maps[table_name]
            
        for index_type, index_columns in indexes.items():
            if index_type == 'primary_key':
                pk_col_name = index_columns['column_name']

                connection = engine.connect()
                connection.execute(
                    f'ALTER TABLE {table_name} add primary key({pk_col_name})')
            if index_type == 'indexes':
                for col in index_columns:
                    index_name = col['index_name']
                    column_name = col['column_name']
                    model_index = Index(
                        index_name, getattr(model, column_name))
                    model_index.create(bind=engine)
            if not is_flat:
                if index_type == 'foreign_key':
                    raise NotImplementedError


index_cache = IndexCache()
