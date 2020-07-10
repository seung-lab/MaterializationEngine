
try:    
    from pychunkedgraph.graph import chunkedgraph
except:
    from pychunkedgraph.backend import chunkedgraph

# TODO: put this in the infoservice

chunkedgraph_version_mapping = {
    "minnie3_v1": 2,
    "fly_v26": 1,
    "fly_v31": 1
}

class ChunkedGraphGateway(object):
    def __init__(self, table_id):
        assert table_id in chunkedgraph_version_mapping
        
        self.cg = init_pcg(table_id)

    def init_pcg(self, table_id):
        if chunkedgraph_version_mapping[table_id] == 1:
            return chunkedgraph.ChunkedGraph(table_id=table_id)
        elif chunkedgraph_version_mapping[table_id] == 2:
            return chunkedgraph.ChunkedGraph(graph_id=table_id)
             
    def get_roots(self, sv_ids, time_stamp):
        return self.cg.get_roots(node_ids=sv_ids, 
                                 time_stamp=time_stamp)
    
    def get_proofread_root_ids(self, start_timestamp, end_timestamp):
        return self.cg.get_proofread_root_ids(start_time=start_timestamp,
                                              end_time=end_timestamp)
