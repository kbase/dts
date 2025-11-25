# CyVerse iRODS S3 Proxy Server

This directory contains a Docker image and configuration files for a proxy server that exposes the
iRODS host at data.cyverse.org as an S3 host. This allows us to transfer files between iRODS and S3
seamlessly.

## Building Custom Plugins

The proxy server uses dynamically-loadable modules ("plugins") for [mapping S3 buckets to iRODS collections](https://github.com/irods/irods_client_s3_api?tab=readme-ov-file#bucket-mapping)
and [mapping S3 credentials to iRODS users](https://github.com/irods/irods_client_s3_api?tab=readme-ov-file#user-mapping).
The server includes some basic plugins that can read these mappings from JSON files.

We have developed replacements for these plugins that expand environment variables in our config
files, removing the need to store credentials in plain text. If you look at the [bucket-mapping.json](bucket-mapping.json)
and [user-mapping.json](user-mapping.json) plugin configuration files in this directory, you'll see
environment variables that must be set within the container environment:

* `IRODS_COLLECTION`: the path to the iRODS collection hosted by the proxy server
* `IRODS_USERNAME`: the iRODS user providing access to the collection hosted by the proxy server
* `S3_ACCESS_KEY_ID`: the S3 access key ID used to access the S3 API when interacting with the proxy server
* `S3_SECRET_KEY`: the S3 secret key used to authenticate with the S3 API
* `S3_BUCKET_NAME`: the name of the S3 bucket in which the iRODS collection is exposed

To build the custom plugins, use [CMake](https://cmake.org):

```sh
cmake -S . -B build
cmake --build build
```

These commands produce a `build/` directory containing plugins named `libirods_s3_api_bucket_mapper.so`
and `libirods_s3_api_user_mapper.so`. You'll mount this `build/` directory as `/plugins` within the
container in the next step so the server can reach them. See [config.json](config.json) for details.

## Running the Server in a Container

From this directory, run the following command, substituting values for each environment variable:

```sh
docker run -d --rm --name irods_s3_api \
    -e IRODS_COLLECTION='<collection-path>' \
    -e IRODS_USERNAME='<irods-user>' \
    -e S3_ACCESS_KEY_ID='<s3-access-key-id>' \
    -e S3_SECRET_KEY='<s3-secret-key>' \
    -e S3_BUCKET_NAME='<s3-bucket-name>' \
    -v config.json:/config.json:ro \
    -v bucket-mapping.json:/bucket-mapping.json:ro \
    -v user-mapping.json:/user-mapping.json:ro \
    -v build:/plugins:ro \
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
