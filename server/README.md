# Server
This module contains all code related to the server module of the framework.

## Core
Core contains shared code and tests for the rest of the server components. This includes:
- Domain Specific Language (Field Expressions, Comparisons, Aggregate Expressions)
- Query Plan (Generation and Execution)
- Table Model (Tables, Data Sources, etc)

## Orchestrator
Orchestrator contains code and tests specific to the main orchestrator node of the cluster. This handles receiving requests from users, and sending parts of the request to each worker node to perform the computation.

Details of building and executing the orchestrator node can be found [here.](./orchestrator/README.md)

## Worker
Worker contains code and tests specific to actually performing computation on part of the full dataset.

Details of building and executing the worker node can be found [here.](./worker/README.md)