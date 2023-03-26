# Worker

This module contains code and tests specific to the worker server implementation.

## Requirements
- sbt must be installed with either Java 8 or 11.
- Docker Desktop must be installed to build a docker image.

## Usage
- `sbt run` will start a worker under the default port of 50052.
- `sbt "run {port}"` will start a worker under a given port.

## Docker Build
- `sbt docker:publishLocal` will build a docker image and publish it to the local docker registry.
