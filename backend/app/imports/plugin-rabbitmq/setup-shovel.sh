#!/bin/bash
# Wait for RabbitMQ management API to be ready
until curl -s -u guest:guest http://localhost:15672/api/overview > /dev/null 2>&1; do
  echo "Waiting for RabbitMQ management API..."
  sleep 2
done

echo "RabbitMQ is ready, configuring shovel..."

# Create the shovel to pull from MDH RabbitMQ
curl -s -u guest:guest -X PUT http://localhost:15672/api/parameters/shovel/%2F/mdh-to-local \
  -H "content-type: application/json" \
  -d '{
    "value": {
      "src-protocol": "amqp091",
      "src-uri": "amqp://guest:guest@host.docker.internal:5673",
      "src-exchange": "masterdata-events",
      "src-exchange-key": "",
      "dest-protocol": "amqp091",
      "dest-uri": "amqp://guest:guest@localhost",
      "dest-exchange": "masterdata-events",
      "ack-mode": "on-confirm",
      "reconnect-delay": 5
    }
  }'

echo "Shovel configured!"
