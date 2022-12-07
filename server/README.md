# Server Module
This module contains all code related to the server module of the framework.

## Core
Core contains shared code for the rest of the server components. This includes the Table model implementation in Scala, field expressions, and code to actually evaluate these.

## Orchestrator
Orchestrator contains code specific to the main orchestrator node of the cluster. This handles receiving requests from users, and sending parts of the request to each worker node to perform the computation.

## Worker
Worker contains code specific to actually performing computation on part of the full dataset.

## Kubernetes
This folder contains .yaml files required for initialising the Kubernetes cluster.