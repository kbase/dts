#!/bin/bash
set -e

echo "Waiting for PostgreSQL to be ready..."
until PGPASSWORD="${IRODS_DB_PASSWORD}" pg_isready -h "${IRODS_DB_HOST}" -p "${IRODS_DB_PORT}" -U "${IRODS_DB_USER}"; do
  echo "PostgreSQL is not ready yet. Sleeping..."
  sleep 2
done

echo "PostgreSQL is ready. Starting iRODS server..."
if [ ! -f /var/lib/irods/.irods/irods_environment.json ]; then
    echo "iRODS not initialized. Running setup..."
    python3 /var/lib/irods/scripts/setup_irods.py --json_configuration_file /provider-config.json
    echo "iRODS setup completed."
else
    echo "iRODS already initialized."
fi

# Start the iRODS server
echo "Starting iRODS server..."
su - irods -c "/usr/sbin/irodsServer -d"

# Wait for iRODS server to start
echo "Waiting for iRODS server to start..."
for i in {1..30}; do
    if su - irods -c "ils" 2>/dev/null; then
        echo "iRODS server is up and running."
        break
    fi
    if [ $i -eq 30 ]; then
        echo "iRODS server failed to start within expected time."
        exit 1
    fi
    sleep 2
done

# Put a sample file into iRODS and verify its presence
echo "Putting a sample file into iRODS..."
echo "This is a sample file for iRODS." > /tmp/sample_file.txt
su - irods -c "iput /tmp/sample_file.txt /tempZone/home/rods/sample_file.txt"
echo "Verifying the sample file in iRODS..."
if su - irods -c "ils /tempZone/home/rods/sample_file.txt" >/dev/null 2>&1; then
    echo "Sample file successfully uploaded to iRODS."
    echo "Checking contents of the sample file in iRODS:"
    su - irods -c "iget /tempZone/home/rods/sample_file.txt /tmp/retrieved_sample_file.txt"
    if cmp -s /tmp/sample_file.txt /tmp/retrieved_sample_file.txt; then
      echo "File contents match."
    else
      echo "File contents do not match!"
      exit 1
    fi
else
    echo "Failed to upload sample file to iRODS."
    exit 1
fi

# Make sure the collections expected by the S3 tests exist
su - irods -c "imkdir -p /tempZone/home/rods/test-bucket"
su - irods -c "imkdir -p /tempZone/home/rods/test-bucket-integration-irods-foo"
su - irods -c "imkdir -p /tempZone/home/rods/test-bucket-integration-irods-bar"
su - irods -c "imkdir -p /tempZone/home/rods/test-bucket-integration-irods-baz"

tail -f /var/lib/irods/log/rodsLog.* 2>/dev/null || tail -f /dev/null