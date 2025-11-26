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

tail -f /var/lib/irods/log/rodsLog.* 2>/dev/null || tail -f /dev/null