from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

for c in w.clusters.list():
  
  if 'Standard_' in c.cluster_name:
    print(f"Status '{c.cluster_name}' state='{c.state}'")
    print(f"\tstate_message='{c.state_message}'\n")