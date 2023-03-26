# Orchestrator

This module contains code and tests specific to the orchestrator server implementation.

## Requirements
- sbt must be installed with either Java 8 or 11.
- Docker Desktop must be installed to build a docker image.

## Usage
- `sbt run` will start an orchestrator on the port 50051.

## Docker Build
- `sbt docker:publishLocal` will build a docker image and publish it to the local docker registry.
