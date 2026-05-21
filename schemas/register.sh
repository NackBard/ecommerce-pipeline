#!/bin/sh

set -e

echo "Installing dependencies..."
apk add --no-cache curl jq

echo "Registering Avro schema..."
cd /schemas

SCHEMA_JSON=$(cat ecommerce-event.avsc | jq -c . | jq -R -s .)

RESPONSE=$(curl -s -w "%{http_code}" -o /tmp/response.json \
  -X POST http://schema-registry:8081/subjects/ecommerce-events-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "{\"schemaType\": \"AVRO\", \"schema\": ${SCHEMA_JSON}}")

if [ "$RESPONSE" = "200" ] || [ "$RESPONSE" = "201" ]; then
  echo "✓ Schema registered successfully"
  cat /tmp/response.json
  exit 0
else
  echo "✗ Failed with HTTP $RESPONSE"
  cat /tmp/response.json
  exit 1
fi