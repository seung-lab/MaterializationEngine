import os


def get_app_base_path():
    return os.path.dirname(os.path.realpath(__file__))


def get_instance_folder_path():
    return os.path.join(get_app_base_path(), 'instance')


def make_root_id_column_name(column_name: str):
    pos = column_name.split("_")[0]
    pos_type = column_name.split("_")[1]
    return f"{pos}_{pos_type}_root_id"


def build_materailized_table_id(aligned_volume: str, table_name: str) -> str:
    return f"mat__{aligned_volume}__{table_name}"
