#!/bin/bash

set -e

# Create a temporary directory
TEMP_DIR=$(mktemp -d)
trap 'rm -rf "$TEMP_DIR"' EXIT

# Define file paths
OPENAPI_URL=${OPENAPI_URL:-"https://raw.githubusercontent.com/tari-project/minotari-cli/refs/heads/main/openapi.json"}
OPENAPI_SPEC="$TEMP_DIR/openapi.json"
CLIENT_CRATE_DIR="minotari-client"

# Download the OpenAPI specification
echo "Downloading OpenAPI spec from $OPENAPI_URL..."
curl -o "$OPENAPI_SPEC" "$OPENAPI_URL"

# Generate the client code
echo "Generating client from $OPENAPI_SPEC..."
openapi-generator generate \
  -g rust \
  -i "$OPENAPI_SPEC" \
  -o "$TEMP_DIR/generated_client" \
  --skip-validate-spec

# Replace the old client source code
echo "Replacing old client source code..."
rm -rf "$CLIENT_CRATE_DIR/src"
mv "$TEMP_DIR/generated_client/src" "$CLIENT_CRATE_DIR/src"

# Prepend allow warnings to lib.rs
echo "Prepending allow warnings to $CLIENT_CRATE_DIR/src/lib.rs..."
sed -i '' '1s/^/#![allow(warnings)]\n#![allow(clippy::all)]\n/' "$CLIENT_CRATE_DIR/src/lib.rs"

echo "API client regenerated successfully in $CLIENT_CRATE_DIR/"
