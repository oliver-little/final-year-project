# Kubernetes 
This folder contains scripts and yaml files for executing the framework on Kubernetes.

- The `azure` folder contains a script for performing first-time setup of the Kubernetes Cluster. It also contains a yaml file for defining the K8ssandra Cluster.
- The `configs` folder contains yaml file definitions for the orchestrator Deployment and Service, as well as the worker StatefulSet and Service.

## Execution
Testing on Kubernetes was performed using [Azure Kubernetes Service](https://azure.microsoft.com/en-us/products/kubernetes-service), and as such all yaml files in the repository are provided with this in mind. Changes will need to be made for alternative cloud providers.

1. Create an instance of Azure Kubernetes Service
    - NOTE: each node should have at least 2 vCores and 8GB RAM available.
    
2. Run [create_azure_cluster.sh](./azure/create_azure_cluster.sh). The number of Cassandra nodes in the cluster can be customised in [demo-cluster.yaml](./azure/demo_cluster.yaml), line 24.
    - NOTE: there should be as many Kubernetes nodes as the number of Cassandra nodes, and each node should have 4GB RAM free for Cassandra.

3. Build docker images for the python_client, and the orchestrator and worker nodes. Instructions for each of the components can be found in the following README files: 
    - [python_client](../python_client/README.md) 
    - [orchestrator](../server/orchestrator/README.md)
    - [worker](../server/worker/README.md) 

4. Store the built docker images in a container registry which the Kubernetes service can access. [Azure Container Registry](https://azure.microsoft.com/en-us/products/container-registry) can be configured to allow access from the Kubernetes service.

5. Edit the yaml file for the [worker:](./configs/worker.yaml) 
    - Change the number of replicas (line 8) to the desired amount - 1 worker per node is the recommended.
    - Change the image (line 20) to point to the chosen container registry.
    - Change the resource requests and limits to the chosen amount - generally, the more CPU and memory available, the better.
    - *Required only if changes were made to demo-cluster.yaml*  
    Change the following variables to match the configuration of [demo-cluster.yaml](./azure/demo_cluster.yaml):
        - Datacenter Name (line 37)
        - Cassandra All Pods Service Base URL (line 43): `{cluster-name}-{datacenter-name}-all-pods-service`
        - Number of Cassandra Nodes (line 45)
        - Cassandra Node Name (line 47): `{cluster-name}-{datacenter-name}-default-sts`
        - Cassandra Authentication Secret (line 54): `{cluster-name}-superuser`
        - Pod Affinity Cluster (line 70): `{cluster-name}`
        - Pod Affinity Datacenter (line 75): `{datacenter-name}`

6. Edit the yaml file for the [orchestrator:](./configs/orchestrator.yaml)
    - Change the image (line 19) to point to the chosen container registry.
    - Change the number of workers to the number of configured replicas (line 40)
    - *Required only if changes were made to demo-cluster.yaml*  
    Change the following variables to match the configuration of [demo-cluster.yaml](./azure/demo_cluster.yaml):
        - Cassandra Service URL (line 27): `{cluster-name}-{datacenter-name}-service`
        - Datacenter Name (line 29)
        - Cassandra Authentication Secret (line 44): `{cluster-name}-superuser`

7. Apply the orchestrator and worker files using `kubectl`:
    - `kubectl apply -f orchestrator.yaml`
    - `kubectl apply -f worker.yaml`

8. Kubernetes will create the appropriate pods, and the orchestrator will be available at orchestrator-service inside the cluster. Follow [Python Instructions](#python-instructions) below in order to create an interactive pod to execute commands in.

## Python Instructions
The following command will start bash in an interactive container containing the python code.   
- `kubectl run python-shell --rm -i --tty --image oliverlittle.azurecr.io/python_client -- bash`

Replace `oliverlittle.azurecr.io` with your chosen container registry. From here, you can generate new data and import it into the server.  

To run inserts, you will need access to the Cassandra username and password:
- This command will get the username, although it's likely that this will be demo-superuser:  
`kubectl get secrets/demo-superuser --template="{{.data.username}}" | base64 -d`

- This command will get the password (randomised per cluster):  
    `kubectl get secrets/demo-superuser --template="{{.data.password}}" | base64 -d`

Python can be started from the entrypoint of the interactive container as follows:  
- `python -i main.py`

From there, create a CassandraConnector:  
- `connector = CassandraConnector("demo-dc1-service", 9042, {username}, {password})`

Run an insert:  
- `CassandraUploadHandler(connector).create_from_csv("/path/to/file.csv", "keyspace", "table", ["partition", "keys])`

Query the cluster:  
- `ClusterManager("orchestrator-service").cassandra_table("keyspace", "table").evaluate()`