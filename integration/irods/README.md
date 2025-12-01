# Integration tests for the Data Transfer Service with iRODS

This folder contains end-to-end tests of the DTS. The tests use a combination of local
and S3 endpoints, with S3 endpoints backed by a MinIO instance and an iRODS S3 proxy server.

To run the tests, you'll need to set up a Minio S3-compatible server. You can do this
using Docker or Podman:

```
docker run -d -p 9000:9000 -p 9001:9001 -e "MINIO_ROOT_USER=minioadmin" -e "MINIO_ROOT_PASSWORD=minioadmin" minio/minio server /data --console-address ":9001"
```

You will also need to start up the test iRODS environment:
```
cd etc/irods
docker compose up -d
```

Then, you can run the integration tests with (from the root project folder):

```
go test ./integration/...
```

You can add a `-v` flag to see output from the tests.

To spin down Minio:
```
docker container stop abc1234
```
(where `abc1234` is the container ID returned when you started the Minio)

You can also inspect the state of the test containers by navigating to http://localhost:9001 and entering the
login credentials you used to start the MinIO container.

To spin down the iRODS test environment:
```
docker compose down -v
```
