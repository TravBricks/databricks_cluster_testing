import constants
import requests
import json 
import re
from pprint import pprint


class cluster_testing:
    def __init__(self):
        self.config = {
            "host":constants.HOST,
            "token":constants.TOKEN,
            "check-exists":True, # True = avoid dupes
            "enforce-create-limit":False, # True = only create `create-limit` quantity
            "create-limit":10, # integer = create x amount of clusters if `enforce-create-limit` == True
            "skip-gpu":True, # True = No clusters with GPU will be tested
            "skip-pattern":r"A10|A100|DC\d{1,2}as_v5|EC\d{1,2}ads_v5|EC\d{1,2}as_v5|E\d{1,2}ids_v4|E\d{1,2}is_v4|H\d{1,2}", # RegEx pattern, add new patterns with | delimiter, if search == True will be skipped
            "test-mode":False, # True = Will not create clusters
        }
        self.instance_types = None;
        self.cluster_test_list = None;
        self.cluster_current_info = None;
    

    def collect(self):
        ## Get all current instance types from workspace
        self.get_instance_types()

        ## Build our list fo smallest instance types
        self.smallest_instances()

        ## Add our API cluster JSON defs
        self.build_cluster_defs()

        ## Get workspace current workspace cluster info
        self.get_clusters_current()


    def build(self):
        
        print('\n\n[Info] Build our clusters')

        ## Build our clusters
        skip_pattern = self.config["skip-pattern"]
        pattern = re.compile(skip_pattern)
        created = 0
        for c in self.cluster_test_list.keys():
            
            if self.config['enforce-create-limit'] and created >= self.config['create-limit']:
                print(f"[Info] Stopping: The create limit has been reached {self.config['create-limit']}")
                break

            if self.config['check-exists'] and c in self.cluster_current_info.keys() != None:
                print(f"[Info] Skipping: '{c}' cluster already exists with id='{self.cluster_current_info[c]}'")
                created += 1 #increment for limit
                continue

            if self.config['skip-gpu'] == True and self.cluster_test_list[c]['num_gpus'] > 0:
                print(f"[Info] Skipping: '{c}' for skip-gpu == True")
                continue
            
            if pattern.search(self.cluster_test_list[c]['node_type_id']):
                print(f"[Info] Skipping: '{c}' for pattern match on '{self.cluster_test_list[c]['node_type_id']}' = {pattern.search(self.cluster_test_list[c]['node_type_id']) is not None}")
                continue
            
            if self.config['test-mode'] == False:
                print(f"[Info] Creating cluster '{c}'")
                result = self.cluster_create(cluster_def=self.cluster_test_list[c]['cluster_def_json'])
                print(f"[Info] Result {result.json()}")
            else:
                print(f"[Info] Test mode: Would have created cluster '{c}'")

            created += 1



    ## Collect related methods
    def get_instance_types(self):
        print(f"[Info] Getting instance types from Clusters API")
        try:
            response = requests.get(
                f"https://{self.config['host']}/api/2.0/clusters/list-node-types",
                headers={"Authorization": f"Bearer {self.config['token']}"},
                json={}
                )
    
            if 'node_types' in response.json().keys():
                self.instance_types = response.json()['node_types']
            else:
                raise TypeError
        
            print(f"[Info] There are {len(self.instance_types)} instance types")
        except:
            print("[Error] No instance types were retrieved or there was an error")
            quit()

    def smallest_instances(self):
        print(f"[Info] Finding smallest size for each instance type")
        recap = r"(Standard_)([A-z]+)(\d{1,3})(.*)$"
        
        x = re.compile(recap)
        
        self.cluster_test_list = {}

        for i in self.instance_types:
            m = x.match(i['node_type_id'])
            
            instance_class = ''.join(m.group(1,2,4))
            instance_size = int(m.group(3))

            is_new_or_lower = False
            if instance_class in self.cluster_test_list.keys():
                if self.cluster_test_list[instance_class]["instance_size"] > instance_size:
                    is_new_or_lower = True
            else:
                is_new_or_lower = True

            if is_new_or_lower:
                self.cluster_test_list[instance_class] = { "node_type_id" : i['node_type_id'],
                                                    "instance_size" : instance_size,
                                                    "category" : i['category'],
                                                    "memory_mb": i['memory_mb'],
                                                    "num_cores": i['num_cores'],
                                                    "num_gpus": i['num_gpus']
                                                    }
        print(f"[Info] There are {len(self.cluster_test_list)} unique small instance types")


    def build_cluster_defs(self):
        print(f"[Info] Adding json cluster definition for API based creation")
        for c in self.cluster_test_list.keys():

            instance_type = c
            node_type_id = self.cluster_test_list[instance_type]['node_type_id']

            self.cluster_test_list[instance_type]['cluster_def_json'] = {
                    "cluster_name": instance_type,
                    "spark_version": "13.3.x-scala2.12",
                    "node_type_id": node_type_id,
                    "num_workers": 0,
                    "autotermination_minutes":20
                    }

    def get_clusters_current(self):
        try:
            response = requests.get(
                f"https://{self.config['host']}/api/2.0/clusters/list",
                headers={"Authorization": f"Bearer {self.config['token']}"},
                json={}
                )
            
            ## Get list of current clusters
            clusters_current = response.json()['clusters']
            
            self.cluster_current_info = {}

            for c in clusters_current:
                self.cluster_current_info[c['cluster_name']] = c['cluster_id']

        except:
            print("[Error] Unable to get current workspace clusters")
            quit()

    def cluster_create(self,cluster_def):
        try:
            return requests.post(
                f"https://{self.config['host']}/api/2.0/clusters/create",
                headers={"Authorization": f"Bearer {self.config['token']}"},
                json=cluster_def
                )
        except:
            return None




def main():

    testing = cluster_testing()

    testing.collect()

    testing.build()
    
        
    
if __name__ == "__main__":
    main()