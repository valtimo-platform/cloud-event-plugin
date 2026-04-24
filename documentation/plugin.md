# Cloud Event Plugin

Send and receive [CloudEvents](https://cloudevents.io/) via RabbitMQ using the Valtimo outbox and inbox.

## Overview

The Cloud Event Plugin integrates Valtimo processes with event-driven architectures. It allows BPMN processes to:

- **Publish** CloudEvents to RabbitMQ via the Valtimo outbox
- **Receive** CloudEvents from RabbitMQ via the Valtimo inbox and route them to waiting or new process instances

## Plugin configuration

| Property | Description                                                                                                          |
|----------|----------------------------------------------------------------------------------------------------------------------|
| *None*   | This plugin has no global configuration properties. All configuration is done at the action level via process links. |

## Actions

### Publish Cloud Event

Publishes a CloudEvent to RabbitMQ via the Valtimo outbox.

**Key:** `publish-cloud-event`

**Supported BPMN elements:**

- Send Task
- Intermediate Throw Event

**Properties:**

| Property     | Description                                      |
|--------------|--------------------------------------------------|
| `eventType`  | The CloudEvent `type` field                      |
| `resultType` | Classification of the event result               |
| `resultId`   | Reference ID for the event result                |
| `data`       | Event payload. Serialized to JSON automatically. |

### Receive Cloud Event

Receives a CloudEvent from the Valtimo inbox (RabbitMQ) and continues a waiting process or starts a new one.

**Key:** `receive-cloud-event`

**Supported BPMN elements:**

- Receive Task
- Intermediate Catch Event (Message)
- Message Start Event

**Properties:**

| Property    | Description                                                                |
|-------------|----------------------------------------------------------------------------|
| `eventType` | Only process events matching this type. If empty, all events are accepted. |

## Process variables

When a CloudEvent is received, the following process variables are set on the target execution:

| Variable               | Description                 |
|------------------------|-----------------------------|
| `cloudEventId`         | Unique ID of the CloudEvent |
| `cloudEventType`       | The CloudEvent `type` field |
| `cloudEventDate`       | Timestamp of the event      |
| `cloudEventResultType` | Result type classification  |
| `cloudEventResultId`   | Result reference ID         |
| `cloudEventData`       | The event payload / result  |

## How it works

### Publishing

1. A Send Task or Intermediate Throw Event triggers the `publish-cloud-event` action.
2. The plugin wraps the data in a `CloudEventPluginEvent` and sends it via the `OutboxService`.
3. The outbox delivers the event to RabbitMQ.

### Receiving

1. An incoming event arrives from RabbitMQ via the Valtimo inbox.
2. The listener checks if the event ID has been processed before (deduplication).
3. All process links using the `receive-cloud-event` action are retrieved.
4. Each process link is checked against the optional `eventType` filter.
5. Depending on the BPMN element type:
    - **Receive Task** -- the waiting execution is signaled with `runtimeService.signal()`.
    - **Intermediate Catch Event** -- a message is correlated to the waiting execution using the BPMN message name.
    - **Message Start Event** -- a new process instance is started. For document processes, a new document is created
      and the process is started via `ProcessDocumentService`. For system processes, the message is correlated directly.

## Database

The plugin creates a `cloud_event_processed` table to track processed event IDs and prevent duplicate handling.

| Column         | Description                     |
|----------------|---------------------------------|
| `event_id`     | Primary key. The CloudEvent ID. |
| `processed_at` | When the event was processed.   |

The schema is managed via Liquibase (see `config/liquibase/cloud-event-master.xml`).
