#!/bin/bash

set -e

# Delete the existing database
echo "Deleting existing database..."
bun run wrangler d1 delete guber-db --skip-confirmation

# Create a new database and capture the output
echo "Creating new database..."
output=$(bun run wrangler d1 create guber-db)

# Extract the database ID from the output
database_id=$(echo "$output" | grep -o 'database_id = "[^"]*"' | cut -d'"' -f2)

if [ -z "$database_id" ]; then
    echo "Error: Could not extract database ID from wrangler output"
    exit 1
fi

echo "New database ID: $database_id"

# Update wrangler.toml with the new database ID
sed -i '' "s/database_id = \"[^\"]*\"/database_id = \"$database_id\"/" wrangler.toml

echo "Updated wrangler.toml with new database ID"

# Run migrations
echo "Running migrations..."
bun run wrangler d1 migrations apply guber-db --local

echo "Database reset complete!"
