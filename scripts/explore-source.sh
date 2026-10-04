#!/usr/bin/env bash
set -Eeuo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if [[ ! -f .env ]]; then
  echo "Missing .env. Copy .env.example first." >&2
  exit 1
fi
set -a
# shellcheck disable=SC1091
source .env
set +a
: "${MSSQL_SA_PASSWORD:?MSSQL_SA_PASSWORD must be set in .env}"

mkdir -p artifacts
docker exec -i adventureworks-sql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "${MSSQL_SA_PASSWORD}" -C -b -W -w 240 \
  < sql/source/profile-source.sql | tee artifacts/source-profile.txt
