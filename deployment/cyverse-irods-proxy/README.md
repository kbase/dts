# CyVerse iRODS S3 Proxy Server

This directory contains a Docker image and configuration files for a proxy server that exposes the
iRODS host at data.cyverse.org as an S3 host. This allows us to transfer files between iRODS and S3
seamlessly.

## Building Custom Plugins for [Bucket](https://github.com/irods/irods_client_s3_api?tab=readme-ov-file#bucket-mapping) and [User]( (https://github.com/irods/irods_client_s3_api?tab=readme-ov-file#user-mapping) Mapping

Our custom plugins allow us to expand environment variables in our config files, removing the need
to store credentials in text files.

## Running the Server in a Container

From this directory, run the following command:

```sh
docker run -d --rm --name irods_s3_api \
    -v config.json:/config.json:ro \
    -v bucket-mapping.json:/bucket-mapping.json:ro \
    -v user-mapping.json:/user-mapping.json:ro \
    -p 9000:9000 \
    coherellc/irods_s3_api:0.5.0
```

### NOTE About Secure Communication

The proxy [does not support SSL/TLS communication](https://github.com/irods/irods_client_s3_api?tab=readme-ov-file#secure-communication-ssltls),
so it should be run behind a reverse-proxy like `nginx`. For practical purposes, we can run the
proxy alongside the DTS behind such a reverse-proxy so all communication is invisible to the outside
world.

## Proxy Server and Docker Image

* [GitHuB Repository](https://github.com/irods/irods_client_s3_api)
* [Docker Image](https://hub.docker.com/r/coherellc/irods_s3_api)

## Files in the Directory

* `bucket-mapping.json`: a [file mapping S3 buckets to iRODS collections](https://github.com/irods/irods_client_s3_api?tab=readme-ov-file#bucket-mapping)
* `config.json`: a [JSON config file](https://github.com/irods/irods_client_s3_api?tab=readme-ov-file#configuration)
  for the proxy server
* `Make
* `plugins/`: a directory containing custom plugins for bucket mapping and user mapping. Type `make plugins` to build these.
* `user-mapping.json`: a [file mapping S3 access keys to iRODS credentials](https://github.com/irods/irods_client_s3_api?tab=readme-ov-file#user-mapping)

The configuration data was copied from [this page](https://learning.cyverse.org/ds/gocommands/configuration/#using-the-init-command).
