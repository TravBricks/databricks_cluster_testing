from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

for c in w.clusters.list():
  if 'Standard_' in c.cluster_name:
     print(f"Deleting '{c.cluster_name}' id={c.cluster_id}")
     w.clusters.delete(cluster_id=c.cluster_id).result()