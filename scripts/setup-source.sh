#!/usr/bin/env bash
set -Eeuo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly BACKUP_NAME="AdventureWorks2022.bak"
readonly BACKUP_URL="https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/${BACKUP_NAME}"
readonly CONTAINER_NAME="adventureworks-sql"

cd "${ROOT_DIR}"

if [[ ! -f .env ]]; then
  echo "Missing .env. Run 'cp .env.example .env' and set a strong local password." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
source .env
set +a
: "${MSSQL_SA_PASSWORD:?MSSQL_SA_PASSWORD must be set in .env}"

mkdir -p data
if [[ ! -s "data/${BACKUP_NAME}" ]]; then
  echo "Downloading the official AdventureWorks 2022 OLTP backup..."
  curl --fail --location --retry 3 --output "data/${BACKUP_NAME}.part" "${BACKUP_URL}"
  mv "data/${BACKUP_NAME}.part" "data/${BACKUP_NAME}"
else
  echo "Using existing data/${BACKUP_NAME}."
fi

docker compose up -d sqlserver

sqlcmd_path="/opt/mssql-tools18/bin/sqlcmd"
echo "Waiting for SQL Server to accept connections..."
for attempt in {1..60}; do
  if docker exec "${CONTAINER_NAME}" "${sqlcmd_path}" -S localhost -U sa -P "${MSSQL_SA_PASSWORD}" -C -Q "SELECT 1" -b >/dev/null 2>&1; then
    break
  fi
  if [[ "${attempt}" == 60 ]]; then
    echo "SQL Server did not become ready. Inspect logs with: docker compose logs sqlserver" >&2
    exit 1
  fi
  sleep 2
done

docker exec -i "${CONTAINER_NAME}" "${sqlcmd_path}" \
  -S localhost -U sa -P "${MSSQL_SA_PASSWORD}" -C -b \
  -v BackupFile="/var/opt/mssql/backup/${BACKUP_NAME}" \
  < sql/source/restore-adventureworks.sql

echo "AdventureWorks is ready at localhost:${MSSQL_PORT:-1433}."
