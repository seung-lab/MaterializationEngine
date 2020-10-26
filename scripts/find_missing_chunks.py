from sqlalchemy import create_engine 
import numpy as np
import pandas as pd
import json



def find_missing_chunks_by_ids(file_path, sql_uri, table_name, chunk_size: int=100_000):
    ids = np.load(file_path)
    print(len(ids))

    engine = create_engine(sql_uri)
    start_ids = ids[::100000]
    valstr = ",".join([str(s) for s in start_ids])
    found_ids = pd.read_sql(f"select id from {table_name} where id in ({valstr})", engine)
    chunk_ids = np.where(~np.isin(start_ids, found_ids.id.values))[0] 

    print(chunk_ids)

    chunks = chunk_ids * chunk_size
    c_list = chunks.tolist()
    data = [[itm, chunk_size] for itm in c_list]
    output = json.dumps(data)

    return output

# if __name__ == "__main__":
#     find_missing_chunks_by_ids()