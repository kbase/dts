# Integration tests for the Data Transfer Service

This folder contains end-to-end tests of the DTS. The tests use a combination of local
and S3 endpoints, primarily to test the transfer orchestration.

To run the tests, you'll need to set up a Minio S3-compatible server. You can do this
using Docker or Podman:

```
docker run -d -p 9000:9000 -p 9001:9001 -e "MINIO_ROOT_USER=minioadmin" -e "MINIO_ROOT_PASSWORD=minioadmin" minio/minio server /data --console-address ":9001"
```

Then, you can the integrations tests with:

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
