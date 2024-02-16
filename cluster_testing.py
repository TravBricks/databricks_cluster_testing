import constants
import requests
import json 
import re
from pprint import pprint

# future: use a parser for program arguments
config = {
    "host":constants.HOST,
    "token":constants.TOKEN,
    "check-exists":True, # True = avoid dupes
    "enforce-create-limit":False, # True = only create `create-limit` quantity
    "create-limit":10, # integer = create x amount of clusters if `enforce-create-limit` == True
    "skip-gpu":True, # True = No clusters with GPU will be tested
    "skip-pattern":r"A10|A100|DC\d{1,2}as_v5|EC\d{1,2}ads_v5|EC\d{1,2}as_v5|E\d{1,2}ids_v4|E\d{1,2}is_v4|H\d{1,2}", # RegEx pattern, add new patterns with | delimiter, if search == True will be skipped
    "test-mode":False, # True = Will not create clusters
}

def get_instance_types(config):
    try:
        return requests.get(
            f"https://{config['host']}/api/2.0/clusters/list-node-types",
            headers={"Authorization": f"Bearer {config['token']}"},
            json={}
            )
    except:
        return None

def get_clusters_current(config):
    try:
        return requests.get(
            f"https://{config['host']}/api/2.0/clusters/list",
            headers={"Authorization": f"Bearer {config['token']}"},
            json={}
            )
    except:
        return None

def cluster_create(config,cluster_def):
    try:
        return requests.post(
            f"https://{config['host']}/api/2.0/clusters/create",
            headers={"Authorization": f"Bearer {config['token']}"},
            json=cluster_def
            )
    except:
        return None


def smallest_instances(instance_types):
    recap = r"(Standard_)([A-z]+)(\d{1,3})(.*)$"
    x = re.compile(recap)

    cluster_test_list = {}

    for i in instance_types['node_types']:
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
                "num_workers": 0,
                "autotermination_minutes":20
                }

    return cluster_test_list



def main():

    ## Get all current instance types from workspace
    instance_types_response = get_instance_types(config=config)
    if instance_types_response is None:
        print("No instance types were retrieved or there was an error")
        quit()

    ## Build our list
    cluster_test_list = smallest_instances(instance_types=instance_types_response.json())

    ## Add cluster definition JSON
    cluster_test_list_defs = build_cluster_defs(cluster_test_list=cluster_test_list)

    ## Get list of current clusters
    clusters_current_result = get_clusters_current(config=config)
    clusters_current = clusters_current_result.json()
    cluster_current_info = {}
    if 'clusters' in clusters_current.keys():
        for c in clusters_current['clusters']:
            cluster_current_info[c['cluster_name']] = c['cluster_id']

    ## Build our clusters
    skip_pattern = config["skip-pattern"]
    pattern = re.compile(skip_pattern)
    created = 0
    for c in cluster_test_list_defs.keys():
        
        if config['enforce-create-limit'] and created > config['create-limit']:
            print(f"Stopping: The create limit has been reached {config['create-limit']}")
            break

        if config['check-exists'] and c in cluster_current_info.keys() != None:
                print(f"Skipping: '{c}' cluster already exists with id='{cluster_current_info[c]}'")
                created += 1 #increment for limit
                continue

        if config['skip-gpu'] == True and cluster_test_list_defs[c]['num_gpus'] > 0:
            print(f"Skipping: '{c}' for skip-gpu == True")
            continue
        
        if pattern.search(cluster_test_list_defs[c]['node_type_id']):
            print(f"Skipping: '{c}' for pattern match on '{cluster_test_list_defs[c]['node_type_id']}' = {pattern.search(cluster_test_list_defs[c]['node_type_id']) is not None}")
            continue
        
        if config['test-mode'] == False:
            print(f"Creating cluster '{c}'")
            result = cluster_create(config=config,cluster_def=cluster_test_list_defs[c]['cluster_def_json'])
            print(f"Result {result.json()}")
        else:
            print(f"Test mode: Would have created cluster '{c}'")

        created += 1
    
if __name__ == "__main__":
    main()