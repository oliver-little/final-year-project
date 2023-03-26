# Python Client

This module contains code and tests for the python frontend of the framework.

## Requirements
- Python 3.10 or 3.11 must be installed.
- Install dependencies using `pip install -r requirements.txt`

## Usage
- Start an interactive terminal using `python -i main.py`.
- See the Jupyter Notebooks [cluster.ipynb](./cluster.ipynb) or [project-demonstration.ipynb](./project_demonstration.ipynb) for example code.

## Docker Build
- This directory contains a [dockerfile](./dockerfile) which can build a image to run the python frontend. It contains no command to run automatically, so should be run as an interactive container only.
- Use `docker build -t python_client:latest .` from this directory to build an image.