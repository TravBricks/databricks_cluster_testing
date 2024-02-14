
from pprint import pprint

config = {
    "host":"adb-3180815342576841.1.azuredatabricks.net",
    "token":"dapi7b08c5d2d25408267033d98890fd5689-3",
}

def get_instance_types(config):
    import requests
    try:
        return requests.get(
            f"https://{config['host']}/api/2.0/clusters/list-node-types",
            headers={"Authorization": f"Bearer {config['token']}"},
            json={}
            )
    except:
        return None

def create_cluster(config,cluster_def):
    import requests
    import json
    try:
        return requests.post(
            f"https://{config['host']}/api/2.0/clusters/create",
            headers={"Authorization": f"Bearer {config['token']}"},
            # json=json.dumps(cluster_def,indent=4)
            json=cluster_def
            )
    except:
        return None


def smallest_instances(instance_types):
    import re

    recap = r"(Standard_)([A-z]+)(\d{1,3})(.*)$"
    x = re.compile(recap)

    cluster_test_list = {}

    for i in instance_types['node_types']:
        # print(i['node_type_id'])
        m = x.match(i['node_type_id'])
        
        instance_class = ''.join(m.group(1,2,4))
        instance_size = int(m.group(3))

        is_new_or_lower = False
        if instance_class in cluster_test_list.keys():
            if cluster_test_list[instance_class]["instance_size"] > instance_size:
                is_new_or_lower = True
        else:
            is_new_or_lower = True

        if is_new_or_lower:
            cluster_test_list[instance_class] = { "node_type_id" : i['node_type_id'],
                                                "instance_size" : instance_size,
                                                "category" : i['category'],
                                                "memory_mb": i['memory_mb'],
                                                "num_cores": i['num_cores'],
                                                "num_gpus": i['num_gpus']
                                                }
    return cluster_test_list


def build_cluster_defs(cluster_test_list):
    for c in cluster_test_list.keys():

        instance_type = c
        node_type_id = cluster_test_list[instance_type]['node_type_id']

        cluster_test_list[instance_type]['cluster_def_json'] = {
                "cluster_name": instance_type,
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": node_type_id,
                "num_workers": 0
                }

    return cluster_test_list



def main():

    ## Get all current instance types from workspace
    instance_types_response = get_instance_types(config=config)
    if instance_types_response is None:
        print("No instance types were retrieved or there was an error")
        quit()

    # pprint(instance_types_response.json())    
    
    ## Build our list
    cluster_test_list = smallest_instances(instance_types=instance_types_response.json())

    # pprint(cluster_test_list)

    ## Add cluster definition JSON
    cluster_test_list_defs = build_cluster_defs(cluster_test_list=cluster_test_list)

    # pprint(cluster_test_list_defs)

    ## Build 
    # create_small_clusters()# cluster_test_list
    import json

    for c in cluster_test_list_defs.keys():
        print(f"Creating cluser '{c}'")
        result = create_cluster(config=config,cluster_def=cluster_test_list_defs[c]['cluster_def_json'])
        print(f"Result {result.json()}")
        # print(json.dumps(cluster_test_list_defs[c]['cluster_def_json']))
    
if __name__ == "__main__":
    main()