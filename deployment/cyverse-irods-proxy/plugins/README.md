# CyVerse iRODS-S3 Proxy Plugins

These plugins perform [bucket mapping](https://github.com/irods/irods_client_s3_api?tab=readme-ov-file#bucket-mapping)
and [user mapping](https://github.com/irods/irods_client_s3_api?tab=readme-ov-file#user-mapping) for
the CyVerse iRODS-S3 proxy described in the parent directory. These are [custom plugins](https://github.com/irods/irods_client_s3_api?tab=readme-ov-file#custom-plugins)
that replace the "local-file" versions bundled with the proxy server, expanding environment variables
within the config files to avoid exposing credentials.
