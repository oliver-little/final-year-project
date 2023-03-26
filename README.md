# Final Year Project
This is the code repository for my final year project - a query processing engine over a distributed cluster of nodes. This project was developed between October 2022 and April 2023.

## Contents

There are a number of folders, each relating to a separate part of the project. Details of each folder, as well as execution instructions, are included below:

### Kubernetes
This folder contains .yaml files required for initialising the Kubernetes cluster.

A README file with further details of the contents of this folder can be found [here.](/kubernetes/README.md)

### Protos
This folder contains protobuf definition files, which are used by both the python client, and the orchestrator and worker nodes.

### Python Client
This module contains all code related to the client-side implementation of the framework.

A README file with further details of the contents of this folder can be found [here.](/python_client/README.md)

### Report
This folder contains the full project report, along with all source LaTeX files and images used for generating the report.

### Server
This module contains all code related to the server-side implementation of the framework, including core code, and orchestrator and worker server implementations.

A README file with further details of the contents of this folder can be found [here.](/server/README.md)

## Execution

There are two main ways of executing this project:
- Kubernetes: this is best for use in a production environment.
- Local: this is best for use in testing, but is restricted to execution on a single machine.

### Kubernetes Execution
Detailed instructions for setting up and using a Kubernetes cluster to execute the framework can be found [here.](/kubernetes/README.md)

### Local Execution
Running locally requires the following to be installed:
- Docker Desktop
- sbt, with Java 8 or 11.

Instructions to setup the cluster locally are included below:

1. Start Cassandra on docker, with port 9042 exposed to the local machine using the following command:
    - `docker run -d --name cassandra -p 9042:9042 cassandra`

2. Set the paths to each of the workers in line 10 of [application.conf,](/server/core/src/main/resources/application.conf) and save the file. As this is running locally, each worker will be running on localhost, and each will have to bind to a different port.

3. Run the orchestrator node using `sbt run` from the orchestrator directory: `/server/orchestrator`.

4. Run each of the worker nodes in a separate terminal instance from the worker directory: `/server/worker`. Ensure the ports defined in [application.conf](/server/core/src/main/resources/application.conf) match the ports the workers are created with.
    - For example, if a worker needs to be assigned to port 50051, use the command `sbt "run 50051"`

5. Finally, in a separate terminal instance, start an interactive Python shell using `python -i main.py` from the python_client directory: `/python_client/`.
    - Connect to the orchestrator using `ClusterManager("localhost")`
    - Create a CassandraConnector to insert data using `connector = CassandraConnector("localhost", 9042)`

## Other Notes
A list of dependencies that were used to complete this project are detailed in [CONTRIBUTORS.md](/CONTRIBUTORS.md)