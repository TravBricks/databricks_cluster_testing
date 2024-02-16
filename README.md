# databricks_cluster_testing

## Prequisites:
1. Databricks CLI (v0.18.0 tested)
1. Databricks Python SDK (v0.3.1 tested)

## Setup
Create a constants.py file with the following configuration:
```
HOST = "Your databricks URL"
TOKEN = "Your databricks Access Token"
```

Example
```
HOST = "adb-1234567890.1.azuredatabricks.net"
TOKEN = "dapiabcdefghijklmnop1234565-3"
```

## Main program (Python + Databricks API)
`cluster_testing.py` - uses Databricks API to get current VM instance types for clusters and creates clusters for rapid testing of single-node cluster startup


## Use Databricks Python SDK
https://docs.databricks.com/en/dev-tools/sdk-python.html

*Can be run in Databricks notebook*

`cluster_testing_status.py` - check status of test clusters

`cluster_testing_cleanup.py` - delete test clusters
