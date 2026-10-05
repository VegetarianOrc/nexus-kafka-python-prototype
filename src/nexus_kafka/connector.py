"""Nexus connector: start standalone Nexus operations from Kafka records.

One process runs two parts on one event loop:

1. A Kafka consumer. For each record on an input topic, it starts a standalone
   Nexus operation for each route of the topic. The JSON value of the record is
   the operation input. If the route has an output topic,
   the start request includes a completion callback.
2. A Temporal worker for the callback task queue. When an operation finishes, the
   server calls the on-complete operation of this worker. The operation writes the
   result or the failure to the output topic.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
from dataclasses import dataclass, field
from typing import Any

import nexusrpc.handler
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord
from temporalio import nexus
from temporalio.api.common.v1 import Payload
from temporalio.client import Client, NexusClient
from temporalio.common import NexusOperationIDConflictPolicy, RawValue
from temporalio.exceptions import (
    ApplicationError,
    FailureError,
    NexusOperationAlreadyStartedError,
)
from temporalio.nexus.notifications import OnCompleteRequest, OnCompleteResponse
from temporalio.nexus.notifications.models import (
    OnCompleteRequestResultFailure,
    OnCompleteRequestResultSuccess,
)
from temporalio.worker import Worker

from nexus_kafka.config import ConnectorConfig, RouteConfig, load_config

logger = logging.getLogger("nexus_kafka.connector")

MAX_START_ATTEMPTS = 5


@dataclass
class CallbackContext:
    """Source context of a completion callback.

    The connector resolves the output topic when it starts the operation, so a
    topic from a record header is still known when the result arrives.
    """

    route: str
    operation_id: str
    output_topic: str
    key: str | None
    source_topic: str
    source_partition: int
    source_offset: int
    headers: dict[str, str] = field(default_factory=dict)


def _payload_bytes(payload: Payload) -> bytes | None:
    # Pass the converted bytes through unchanged. For the default converter,
    # dataclasses and dicts are JSON.
    if payload.metadata.get("encoding") == b"binary/null":
        return None
    return payload.data


def _describe_error(err: BaseException) -> dict[str, Any]:
    message = err.message if isinstance(err, FailureError) else str(err)
    described: dict[str, Any] = {"type": type(err).__name__, "message": message}
    if isinstance(err, ApplicationError):
        described["application_type"] = err.type
        described["non_retryable"] = err.non_retryable
    if err.__cause__ is not None:
        described["cause"] = _describe_error(err.__cause__)
    return described


@nexusrpc.handler.service_handler(name="nexus-kafka.connector.Callbacks")
class CallbackHandler:
    """On-complete operation that writes operation outcomes to Kafka."""

    def __init__(self, producer: AIOKafkaProducer) -> None:
        self._producer: AIOKafkaProducer = producer

    @nexusrpc.handler.sync_operation(name="OnComplete")
    async def on_complete(
        self,
        _ctx: nexusrpc.handler.StartOperationContext,
        input: OnCompleteRequest[RawValue, CallbackContext],
    ) -> OnCompleteResponse:
        context = input.source_context
        match input.result:
            case OnCompleteRequestResultSuccess(value=raw):
                status = "success"
                value = _payload_bytes(raw.payload)
            case OnCompleteRequestResultFailure(value=err):
                status = "failure"
                value = json.dumps({"error": _describe_error(err)}).encode()
        headers = [(k, v.encode()) for k, v in context.headers.items()]
        headers += [
            ("nexus-status", status.encode()),
            ("nexus-route", context.route.encode()),
            ("nexus-operation-id", context.operation_id.encode()),
            ("source-topic", context.source_topic.encode()),
            ("source-partition", str(context.source_partition).encode()),
            ("source-offset", str(context.source_offset).encode()),
        ]
        # The server can deliver a callback more than once. Consumers can use the
        # nexus-operation-id header to remove duplicates. If the write fails, the
        # error goes back to the server, which retries the callback.
        await self._producer.send_and_wait(
            context.output_topic,
            value=value,
            key=context.key.encode() if context.key is not None else None,
            headers=headers,
        )
        logger.info(
            "Wrote %s of operation %s to topic %r",
            status,
            context.operation_id,
            context.output_topic,
        )
        return OnCompleteResponse()


def _decode(data: bytes | None) -> str | None:
    return data.decode("utf-8", errors="replace") if data is not None else None


class Connector:
    def __init__(self, config: ConnectorConfig, client: Client) -> None:
        self._config = config
        self._nexus_clients: dict[str, NexusClient[Any]] = {
            r.name: client.create_nexus_client(service=r.service, endpoint=r.endpoint)
            for r in config.routes
        }

    async def _start_operation(self, route: RouteConfig, msg: ConsumerRecord) -> None:
        headers = {k: _decode(v) or "" for k, v in msg.headers}
        key = _decode(msg.key)
        operation_id = f"{route.name}-{msg.topic}-{msg.partition}-{msg.offset}"
        # The record value is the operation input. It must be JSON that matches
        # the input type of the operation.
        try:
            arg = json.loads(msg.value) if msg.value is not None else None
        except ValueError as e:
            logger.error(
                "Record %s/%d/%d is not valid JSON, skipping: %s",
                msg.topic,
                msg.partition,
                msg.offset,
                e,
            )
            return
        output_topic = route.resolve_output_topic(headers)
        callbacks = []
        if output_topic:
            callbacks.append(
                nexus.create_completion_callback(
                    operation=CallbackHandler.on_complete,
                    task_queue=self._config.callbacks.task_queue,
                    source_context=CallbackContext(
                        route=route.name,
                        operation_id=operation_id,
                        output_topic=output_topic,
                        key=key,
                        source_topic=msg.topic,
                        source_partition=msg.partition,
                        source_offset=msg.offset,
                        headers={
                            h: headers[h]
                            for h in route.propagate_headers
                            if h in headers
                        },
                    ),
                )
            )
        for attempt in range(1, MAX_START_ATTEMPTS + 1):
            try:
                # On redelivery of a record whose operation still runs, attach to
                # that operation instead of starting a second one.
                await self._nexus_clients[route.name].start_operation(
                    route.operation,
                    arg,
                    id=operation_id,
                    id_conflict_policy=NexusOperationIDConflictPolicy.USE_EXISTING,
                    schedule_to_close_timeout=route.schedule_to_close_timeout,
                    summary=f"Kafka {msg.topic}/{msg.partition}/{msg.offset}",
                    completion_callbacks=callbacks,
                )
                logger.info(
                    "Started operation %s (%s/%s) output=%s",
                    operation_id,
                    route.service,
                    route.operation,
                    output_topic or "none",
                )
                return
            except NexusOperationAlreadyStartedError:
                logger.info("Operation %s already started, skipping", operation_id)
                return
            except Exception:
                if attempt == MAX_START_ATTEMPTS:
                    logger.exception(
                        "Could not start operation %s after %d attempts, skipping",
                        operation_id,
                        attempt,
                    )
                    return
                logger.warning(
                    "Attempt %d to start operation %s failed, retrying",
                    attempt,
                    operation_id,
                    exc_info=True,
                )
                await asyncio.sleep(min(2**attempt, 30))

    async def _process_partition(self, messages: list[ConsumerRecord]) -> None:
        # The connector starts the records of one partition in order. Partitions
        # run in parallel.
        for msg in messages:
            routes = self._config.routes_for_topic(msg.topic)
            await asyncio.gather(*(self._start_operation(r, msg) for r in routes))

    async def consume(self, consumer: AIOKafkaConsumer, stop: asyncio.Event) -> None:
        while not stop.is_set():
            batches = await consumer.getmany(timeout_ms=1000, max_records=100)
            if not batches:
                continue
            await asyncio.gather(
                *(self._process_partition(m) for m in batches.values())
            )
            # Commit only after every operation of the batch started, so a crash
            # redelivers records instead of losing them.
            await consumer.commit(
                {tp: m[-1].offset + 1 for tp, m in batches.items() if m}
            )


async def run(config: ConnectorConfig) -> None:
    client = await Client.connect(
        config.temporal.address, namespace=config.temporal.namespace
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=config.kafka.bootstrap_servers,
        acks="all",
        enable_idempotence=True,
    )
    consumer = AIOKafkaConsumer(
        *config.input_topics,
        bootstrap_servers=config.kafka.bootstrap_servers,
        group_id=config.kafka.group_id,
        auto_offset_reset=config.kafka.auto_offset_reset,
        enable_auto_commit=False,
    )
    worker = Worker(
        client,
        task_queue=config.callbacks.task_queue,
        nexus_service_handlers=[CallbackHandler(producer)],
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    for route in config.routes:
        if route.output_topic_header:
            output = (
                f"header {route.output_topic_header!r}, fallback {route.output_topic!r}"
            )
        else:
            output = repr(route.output_topic)
        logger.info(
            "Route %r: %r -> %s/%s/%s, output %s",
            route.name,
            route.input_topic,
            route.endpoint,
            route.service,
            route.operation,
            output,
        )

    await producer.start()
    try:
        async with worker:
            logger.info(
                "Callback worker is running on task queue %r",
                config.callbacks.task_queue,
            )
            await consumer.start()
            try:
                await Connector(config, client).consume(consumer, stop)
            finally:
                await consumer.stop()
    finally:
        await producer.stop()
    logger.info("Connector stopped")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--config", default="connector.toml", help="Path of the TOML file"
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    asyncio.run(run(load_config(args.config)))


if __name__ == "__main__":
    main()
