#!/bin/sh
set -e

# If starting the web server, automatically apply all database migrations first
if [ "$1" = "uvicorn" ]; then
    echo "Running database migrations (alembic upgrade head)..."
    alembic upgrade head
    echo "Migrations applied successfully."
fi

exec "$@"
