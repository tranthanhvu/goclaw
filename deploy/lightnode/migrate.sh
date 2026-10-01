#!/usr/bin/env bash
# Migrate local goclaw data (agents, Zalo session, MCP config, memories) to the VPS.
# Run FROM the local Mac that currently runs goclaw.
#
# Usage:
#   ./migrate.sh root@VPS_IP
#
# Prereqs:
#   - VPS already ran setup.sh with SAME GOCLAW_ENCRYPTION_KEY as local .env
#   - (otherwise encrypted fields — MCP headers, channel secrets — will not decrypt)
set -euo pipefail

SSH_TARGET="${1:?usage: ./migrate.sh user@vps-ip}"
REMOTE_DIR="${2:-goclaw}"   # dir on VPS containing docker-compose.yml

LOCAL_ENV="$(cd "$(dirname "$0")/../.." && pwd)/.env"
# shellcheck disable=SC1090
PG_PASS=$(grep '^POSTGRES_PASSWORD=' "$LOCAL_ENV" | cut -d= -f2-)
PG_USER=$(grep '^POSTGRES_USER=' "$LOCAL_ENV" | cut -d= -f2-); PG_USER=${PG_USER:-goclaw}
PG_DB=$(grep '^POSTGRES_DB=' "$LOCAL_ENV" | cut -d= -f2-); PG_DB=${PG_DB:-goclaw}

echo ">>> Dumping local database..."
DUMP=/tmp/goclaw-migrate-$$.sql.gz
docker exec my-goclaw-postgres-1 pg_dump -U "$PG_USER" -d "$PG_DB" --clean --if-exists | gzip > "$DUMP"
echo "    $(du -h "$DUMP" | cut -f1) -> $DUMP"

echo ">>> Copying to VPS..."
scp "$DUMP" "$SSH_TARGET:/tmp/goclaw-migrate.sql.gz"

echo ">>> Restoring on VPS (stack will restart)..."
ssh "$SSH_TARGET" "cd $REMOTE_DIR && \\
  docker compose stop goclaw && \\
  docker compose up -d --no-deps postgres && \\
  sleep 5 && \\
  docker compose exec -T postgres sh -c 'gunzip -c /tmp/goclaw-migrate.sql.gz | psql -U \$POSTGRES_USER -d \$POSTGRES_DB' && \\
  docker compose up -d goclaw && \\
  rm /tmp/goclaw-migrate.sql.gz"

rm -f "$DUMP"
echo ">>> Done. Verify: ssh $SSH_TARGET 'cd $REMOTE_DIR && docker compose ps'"
