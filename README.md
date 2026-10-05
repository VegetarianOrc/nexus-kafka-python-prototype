# Nexus Kafka connector (prototype)

The connector reads records from Kafka input topics and starts a standalone
Temporal Nexus operation for each record. If a route has an output topic, the
connector attaches a Nexus handler completion callback to the operation. When the
operation finishes, the server calls the callback, and the connector writes the
result or the failure to the output topic.

The prototype uses the `amazzeo/nexus-handler-callbacks-client` branch of
[temporalio/sdk-python](https://github.com/temporalio/sdk-python). Only that
branch has handler completion callbacks.

## Parts

| Part | Command | What it does |
|---|---|---|
| Kafka | `docker compose up -d` | Single-node Kafka (KRaft) on `localhost:9092`. Creates the topics of `connector.toml`. |
| Dev server | `uv run nexus-devserver` | Temporal dev server build with handler callbacks on `localhost:7233` (UI on `localhost:8233`). Creates the `text-processor` Nexus endpoint. |
| Text processor | `uv run text-processor` | Separate Temporal worker in the `text_processor` package. It implements the `text-processor` Nexus service and knows nothing about Kafka. Each operation runs a workflow. |
| Connector | `uv run nexus-connector --config connector.toml` | One process. It runs the Kafka consumer, the Kafka producer, and the callback worker. |

```
            ┌──────────────── connector process ────────────────┐
 input  ──► │ Kafka consumer ──start_operation(callback)──┐     │
 topic      │                                             │     │
            │ callback worker ◄── OnComplete ───┐         │     │ ──► output topic
            └───────────────────────────────────┼─────────┼─────┘
                                                │         ▼
                                         Temporal server (Nexus endpoint)
                                                          │
                                                          ▼
                                         text processor: workflow + activity
```

## Flow

1. The consumer reads a record from an input topic.
2. For each route of the topic, the connector resolves the output topic and starts
   the operation `route.service/route.operation` on `route.endpoint`. The JSON
   value of the record is the operation input. The operation ID is
   `<route>-<topic>-<partition>-<offset>`.
3. If an output topic resolves, the start request has a completion callback. The
   callback targets the `OnComplete` operation of the connector's callback task
   queue. Its source context holds the output topic, the record key, the source
   coordinates, and the propagated headers.
4. The connector commits the offsets of a batch after all of its operations start.
5. The text processor starts `ProcessTextWorkflow`. The workflow waits on a
   durable timer, then runs an activity with retries.
6. When the operation finishes, the server calls `OnComplete` on the connector.
   The connector writes one record to the output topic.

## Configuration

`connector.toml` has the sections `[temporal]`, `[kafka]`, `[callbacks]`, and one
or more `[[routes]]`. More than one route can read the same input topic.

| Route key | Required | Description |
|---|---|---|
| `name` | yes | Unique route name. It is part of the operation ID. |
| `input_topic` | yes | Topic to read. |
| `endpoint` | yes | Nexus endpoint name. |
| `service` | yes | Nexus service name. |
| `operation` | yes | Nexus operation name. |
| `schedule_to_close_timeout_seconds` | no | Default 300. |
| `output_topic` | no | Static output topic. With `output_topic_header`, it is the fallback. |
| `output_topic_header` | no | Kafka header that holds the output topic of each record. |
| `propagate_headers` | no | Input headers to copy to the output record. |

If no output topic resolves for a record, the connector starts the operation
without a callback and writes no result.

## Input records

The record value must be JSON that matches the input type of the operation. The
operations of the text processor take `{"text": "..."}`. Kafka keys, headers, and
offsets do not go to the operation. The connector keeps them in the callback
source context and writes them to the output record.

If the value is not valid JSON, the connector logs an error and skips the record.
If the JSON does not match the input type, the operation fails, and a failure
record goes to the output topic.

## Output records

- **Key:** the key of the input record.
- **Value on success:** the operation result as the payload converter encoded it.
  For the text processor, that is JSON.
- **Value on failure:** `{"error": {"type", "message", "application_type", "non_retryable", "cause"}}`.
- **Headers:** the propagated headers, plus `nexus-status` (`success` or
  `failure`), `nexus-route`, `nexus-operation-id`, `source-topic`,
  `source-partition`, and `source-offset`.

## Run it

You need `uv`, Docker, `kcat`, and a Rust toolchain with `protoc`. The first
`uv sync` builds the SDK bridge from source. That takes several minutes.

```sh
uv sync
docker compose up -d

# Use a different terminal for each command
uv run nexus-devserver            # add --db temporal.db to keep state between runs
uv run text-processor
uv run nexus-connector --config connector.toml
```

## Test with kcat

Produce records:

```sh
# Static output topic. The correlation-id header goes to the output record.
echo '{"text":"hello nexus"}' | kcat -P -b localhost:9092 -t transform.requests -k order-1 -H correlation-id=abc-123

# The workflow fails. A failure record goes to transform.results.
echo '{"text":"this one should fail"}' | kcat -P -b localhost:9092 -t transform.requests -k order-2

# The output topic comes from the reply-topic header. The activity fails twice,
# then succeeds.
echo '{"text":"a flaky record"}' | kcat -P -b localhost:9092 -t analyze.requests -k a-1 -H reply-topic=custom.replies

# No reply-topic header, so the result goes to the fallback analyze.results.
echo '{"text":"no reply header"}' | kcat -P -b localhost:9092 -t analyze.requests -k a-2

# No output topic. The operation runs, and no result is written.
echo '{"text":"audit only"}' | kcat -P -b localhost:9092 -t audit.events -k e-1

# The value is not valid JSON. The connector logs an error and skips the record.
echo 'not json' | kcat -P -b localhost:9092 -t transform.requests -k bad-1

# The JSON does not match the input type. A failure record goes to transform.results.
echo '{"wrong":"shape"}' | kcat -P -b localhost:9092 -t transform.requests -k bad-2
```

Read the results:

```sh
kcat -C -b localhost:9092 -t transform.results -e -f 'key=%k headers=%h\n%s\n\n'
kcat -C -b localhost:9092 -t analyze.results   -e -f 'key=%k headers=%h\n%s\n\n'
kcat -C -b localhost:9092 -t custom.replies    -e -f 'key=%k headers=%h\n%s\n\n'
```

The text processor reacts to these words in the input text:

- `fail`: the workflow fails with a non-retryable `InputRejected` error.
- `flaky`: the activity fails on attempts 1 and 2.

The text processor waits 3 seconds on a durable timer by default. Use
`--delay-seconds` to change it. To see durable execution, stop the text processor
during the wait and start it again. The result still arrives.

To process all input records again, stop the connector, set a new `group_id`
in `connector.toml`, and start the connector. To delete all Kafka data, run
`docker compose down`.

## Delivery semantics and limits

- **At-least-once to Kafka.** The server can deliver a callback more than once.
  Use the `nexus-operation-id` header to find duplicates.
- **Redelivered input.** If the connector stops before it commits, it reads the
  records again. If the operation still runs, the start uses
  `USE_EXISTING` and attaches to it. If the operation already finished, the
  operation runs again.
- **Start failures.** The connector tries each start 5 times. Then it logs the
  error and skips the record. It also skips values that are not valid JSON. A
  production connector needs a dead-letter topic.
- **Header-driven topics.** A producer can make the connector write to any topic
  that the connector can access. A production connector needs an allowlist.
- **Throughput.** The connector starts the records of one partition in order and
  the partitions in parallel. It commits after each batch.
- **Text keys and headers.** The connector decodes keys and header values as
  UTF-8. It replaces bytes that are not valid UTF-8.
