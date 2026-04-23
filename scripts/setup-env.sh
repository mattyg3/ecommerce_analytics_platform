#!/bin/bash

FULL_REFRESH=false

if [ "$1" == "--full-refresh" ]; then
  FULL_REFRESH=true
fi

if [ "$FULL_REFRESH" = true ]; then
  echo "⚠️ This will DELETE data-lake and logs completely!"
  read -p "Are you sure? (y/n): " confirm

  if [ "$confirm" != "y" ]; then
    echo "Aborted."
    exit 1
  fi

  rm -rf data-lake
  rm logs/*
  rm control/*
fi

# Prevent running inside Docker
if [ -f /.dockerenv ]; then
  echo "❌ Run this on your host machine, not inside Docker."
  exit 1
fi

echo "🔧 Setting up environment..."

# ----------------------------
# Generate .env (only once)
# ----------------------------
if [ ! -f .env ]; then
  echo "Generating .env file..."
  UID_VAL=$(id -u)
  GID_VAL=$(id -g)

  cat <<EOF > .env
UID=${UID_VAL}
GID=${GID_VAL}
EOF

  echo ".env created with UID=${UID_VAL}, GID=${GID_VAL}"
else
  echo ".env already exists, skipping..."
fi

# ----------------------------
# Create data lake structure
# ----------------------------
echo "📁 Creating data lake structure..."

mkdir -p data-lake/raw/clickstream \
         data-lake/raw/orders \
         data-lake/landing/clickstream \
         data-lake/landing/orders \
         data-lake/checkpoints

# touch data-lake/landing.duckdb
# touch data-lake/bronze.duckdb
# touch data-lake/staging.duckdb
# touch data-lake/marts.duckdb

# ----------------------------
# Fix permissions
# ----------------------------
echo "🔐 Fixing permissions..."

chown -R $(id -u):$(id -g) data-lake
chmod -R 775 data-lake

echo "✅ Setup complete!"