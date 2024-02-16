from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

for c in w.clusters.list():
  
  if 'Standard_' in c.cluster_name:
    state = c.state.value
    print(f"Status '{c.cluster_name}' state='{state}'")
    if state != "RUNNING":
      print(f"state_message='{c.state_message}'\n")