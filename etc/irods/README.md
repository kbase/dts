# iRODS Provider

The files in this folder can be used to run an instance of an iRODS Provider that could be used to test DTS iRODS integrations.

iRODS requires a PostgreSQL DB instance to be available. The docker-compse configuration starts up a PostgreSQL service and an iRODS provider in separate containers.

## Setup

Install dependencies (first time only):
```
uv sync
```

## Run

To start up the containers in detached mode:
```
podman compose up -d --build --force-recreate
```

To run the example script that transfers files to/from the iRODS instance via the Python iRODS package and via the iRODS S3 Proxy Server, run:
```
uv run test_client.py
```

To shut down the containers and remove volumes:
```
podman compose down -v
```


