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

| Property     | Description |
|--------------|-------------|
| `eventType`  | The CloudEvent `type` field |
| `resultType` | Classification of the event result |
| `resultId`   | Reference ID for the event result |
| `data`       | Event payload. A JSON object or array, resolved or typed as JSON text, is published as is; any other value is published as `{"data": <value>}`. |

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
| `eventType` | Only process events matching this type. If empty, all events are accepted, including the events Valtimo publishes itself. Required on a Message Start Event: without it, every event, and the `DocumentCreated` event of each case it starts, would start another process. For the same reason, do not filter a Message Start Event on an event type the started process publishes itself. |

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

### Transport

Events only travel through RabbitMQ when the host application enables the Valtimo outbox
(`valtimo.outbox.enabled: true`) with a RabbitMQ publisher, and binds the inbox consumer (the
`kotlinInboxCloudEventConsumer` function bean) to a RabbitMQ destination with Spring Cloud Stream. See the Valtimo
outbox and inbox documentation for that configuration.

Without it, as in the sample application in this repository, Valtimo delivers every event it publishes in-process,
after the publishing transaction commits, to the inbox handlers of the same application. Nothing is redelivered then:
an event whose handling fails is not handled again.

### Publishing

1. A Send Task or Intermediate Throw Event triggers the `publish-cloud-event` action.
2. The plugin wraps the data in a `CloudEventPluginEvent` and sends it via the `OutboxService`.
3. The outbox delivers the event to RabbitMQ, or in-process (see [Transport](#transport)).

### Receiving

1. An incoming event arrives via the Valtimo inbox.
2. All process links using the `receive-cloud-event` action are retrieved, and checked against their optional
   `eventType` filter. A Message Start Event link without a filter matches nothing, and logs a warning once. An event that
   matches no link is ignored and not recorded.
3. The listener checks if the event ID has been processed before (deduplication). If not, the ID is recorded before
   any link acts on the event, so a concurrent delivery of the same event fails instead of acting a second time, and
   is skipped when it is delivered again.
4. What waits at each matching link is collected before any of it is continued, so an event continues each wait once,
   even when that moves the process on to another activity linked to the same event. A receive task waits once it has
   been entered: an execution parked on an asynchronous continuation before or after it does not count. A wait that
   an earlier link has since ended is skipped, and so is a suspended process instance.
5. Depending on the BPMN element type:
    - **Receive Task** -- the waiting execution is signaled with `runtimeService.signal()`.
    - **Intermediate Catch Event** -- the message the execution is subscribed to is correlated to it, with any
      expression in the BPMN message name resolved, also when the catch event waits behind an event-based gateway.
      Only message catch events wait for a cloud event: a link on a timer or signal catch event never matches.
    - **Message Start Event** -- a new process instance is started. For document processes, a new document is created
      and the start message of the linked element is correlated to it, so the process starts at that event and not at
      whichever start event the engine considers the process's initial activity. For system processes, the message is
      correlated directly.

A Message Start Event link starts nothing, and logs why, when:

- it is on the start event of an event subprocess rather than of the process itself;
- its process definition is suspended;
- it is on a system process that is not the latest deployed version of that process;
- its process is not linked to a case definition, typically because it is an earlier, detached version of the process;
- its case definition is not the active version of that case definition;
- its case definition does not allow initialising a document (`canInitializeDocument` is false);
- its case document cannot be created, for instance because the document definition has required fields, which the
  empty document the case starts with cannot meet.

A start event without a message is skipped with a warning as well. Skipping rather than failing keeps one
misconfigured link from failing the event, and every other link it matches.

All links an event matches are handled in one transaction. An exception from a process that a link continues or
starts, a synchronous service task after a receive task for instance, rolls back the event and everything the other
links did with it. Make the activity after a linked element asynchronous to keep its failures to itself.

An event is not correlated to a particular process instance: it continues every instance that waits at a matching
link, whatever its business key or `resultId`. Nor is it kept for later. An event that arrives before the process
reaches the linked element, or while it is parked on an asynchronous continuation in front of it, finds nothing to
continue, and is not handled again.

### Incoming event format

The Valtimo inbox only passes on CloudEvents without `data`, or whose `data` is JSON in the shape the Valtimo outbox
publishes. Any other event is dropped by the inbox with a warning in its log, before it reaches this plugin.

```json
{
  "userId": "system",
  "roles": [],
  "resultType": "zaak",
  "resultId": "Z-1",
  "result": { "zaakId": "Z-1" }
}
```

When there is `data`, `userId` and `roles` are required, `result` must be a JSON object or array, and the others may be left out. Events are
deduplicated on their `id` alone, so producers must keep ids unique across sources. `cloudEventDate` holds the event's
`time` as a local date-time, without its offset.

## Database

The plugin creates a `cloud_event_processed` table to track processed event IDs and prevent duplicate handling.

| Column         | Description                     |
|----------------|---------------------------------|
| `event_id`     | Primary key. The CloudEvent ID. |
| `processed_at` | When the event was processed.   |

The schema is managed via Liquibase (see `config/liquibase/cloud-event-master.xml`).
