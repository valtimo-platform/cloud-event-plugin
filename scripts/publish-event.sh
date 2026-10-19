#!/usr/bin/env bash
# Publish a CloudEvent JSON file to RabbitMQ
# Usage: ./publish-event.sh event.json [exchange]
set -euo pipefail
FILE="$1"; EXCHANGE="${2:-masterdata-events}"

# Read file content and create API payload
PAYLOAD=$(jq -c '.' "$FILE")
API_BODY=$(jq -n \
  --arg payload "$PAYLOAD" \
  '{
    properties: {
      content_type: "application/json",
      delivery_mode: 2
    },
    routing_key: "",
    payload: $payload,
    payload_encoding: "string"
  }')

curl -sf -u guest:guest \
  -H "content-type: application/json" \
  -X POST "http://localhost:15672/api/exchanges/%2F/${EXCHANGE}/publish" \
  -d "$API_BODY"
echo
