import requests
import numpy as np
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import pprint
import time
server = "https://35.196.20.36:4001/"


def get_root_id(supervoxel_id, server):
    response = session.get(server + "segmentation/1.0/graph/root",
                           json=[supervoxel_id], verify=False)
    root_id = np.fromstring(response.content, dtype=np.uint64, count=1)[0]
    return root_id


dataset = "pinky40"
synapse_number = 97
xybuffer = 30000
zbuffer = 200

session = requests.session()

response = session.get(
    server + "annotation/dataset/{}/synapse/{}".format(dataset, synapse_number), verify=False)
synapse_json = response.json()
supervoxel_id = synapse_json['post_pt']['supervoxel_id']
root_id = get_root_id(supervoxel_id, server)
print('root_id', root_id)

# pos = synapse_json[
# 'post_pt']['position']
# minbox = list(pos)
# minbox[0] -= xybuffer
# minbox[1] -= xybuffer
# minbox[2] -= zbuffer
# maxbox = list(pos)
# maxbox[0] += xybuffer
# maxbox[1] += xybuffer
# maxbox[2] == zbuffer
# box = [minbox, maxbox]
# print('box', box)
url = server + \
    "chunked_annotation/dataset/{}/rootid/{}/synapse".format(dataset, root_id)
before = time.time()
response = session.get(url,  verify=False)
print('query1 time', time.time() - before)
synapses_dict = response.json()
print(len(synapses_dict))
for id_, syn_d in synapses_dict.items():
    post_root = get_root_id(syn_d['post_pt']['supervoxel_id'], server)
    pre_root = get_root_id(syn_d['pre_pt']['supervoxel_id'], server)
    if pre_root == post_root:
        print('id:', id_, 'pre:', pre_root, 'post:', post_root)
    after = time.time()
    print(after - before)
