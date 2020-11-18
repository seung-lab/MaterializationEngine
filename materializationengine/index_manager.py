from sqlalchemy.engine import reflection
from sqlalchemy import Index


class IndexCache:

    def __init__(self):
        self._index_maps = {}

    def get_table_indexes(self, table_name: str, engine):
        if table_name not in self._index_maps:
            insp = reflection.Inspector.from_engine(engine)
            pk_columns = insp.get_pk_constraint(table_name)
            indexed_columns = insp.get_indexes(table_name)
            foreign_keys = insp.get_foreign_keys(table_name)
            index_map = {}

            if pk_columns:
                pk = {
                    'primary_key_name': pk_columns['name'],
                    'column_name': pk_columns['constrained_columns'][0]
                }
                index_map.update({
                    'primary_key': pk
                })
            if indexed_columns:
                indexes = []
                for index in indexed_columns:
                    indx_map = {
                        'index_name': index["name"],
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
                        'foreign_key': fk["name"],
                        'column_name': fk["constrained_columns"][0],
                    }
                    fKeys.append(foreign_keys.copy())
                index_map.update({
                    'foreign_keys': foreign_keys
                })
            self._index_maps[table_name] = index_map
        return self._index_maps[table_name]

    def drop_table_indexes(self, table_name: str, engine):
        if table_name not in self._index_maps:
            indexes = self.get_table_indexes(table_name, engine)
        else:
            indexes = self._index_maps[table_name]
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
            raise KeyError(f"Table: {table_name} not found in index mapping")
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
