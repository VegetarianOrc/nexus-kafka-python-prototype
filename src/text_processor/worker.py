"""Separate Temporal worker that implements the processor Nexus service.

The worker knows nothing about Kafka. Each operation starts a workflow that
simulates durable work on a text: a durable timer, then an activity with retries.
The workflow result is the operation result.

The input text controls the simulation:

- A text that contains "fail" makes the workflow fail with a non-retryable error.
- A text that contains "flaky" makes the activity fail on its first two attempts.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from dataclasses import dataclass
from datetime import timedelta

import nexusrpc.handler
from temporalio import activity, nexus, workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

with workflow.unsafe.imports_passed_through():
    from text_processor.models import ProcessInput, ProcessorService, ProcessResult

logger = logging.getLogger("text_processor.worker")


@dataclass
class ProcessRequest:
    operation: str
    text: str
    delay_seconds: float


@activity.defn
async def process_text(req: ProcessRequest) -> dict:
    info = activity.info()
    text = req.text
    if "flaky" in text.lower() and info.attempt < 3:
        raise RuntimeError(f"Simulated transient failure on attempt {info.attempt}")
    if req.operation == "transform":
        return {"original": text, "upper": text.upper(), "reversed": text[::-1]}
    if req.operation == "analyze":
        return {
            "characters": len(text),
            "words": len(text.split()),
            "lines": len(text.splitlines()),
            "attempts": info.attempt,
        }
    raise ApplicationError(f"Unknown operation {req.operation!r}", non_retryable=True)


@workflow.defn
class ProcessTextWorkflow:
    @workflow.run
    async def run(self, req: ProcessRequest) -> ProcessResult:
        if "fail" in req.text.lower():
            raise ApplicationError(
                "The input text was rejected",
                type="InputRejected",
                non_retryable=True,
            )
        # A durable timer. The workflow survives worker restarts during the wait.
        await workflow.sleep(timedelta(seconds=req.delay_seconds))
        output = await workflow.execute_activity(
            process_text,
            req,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1), backoff_coefficient=1.0
            ),
        )
        return ProcessResult(
            operation=req.operation,
            workflow_id=workflow.info().workflow_id,
            output=output,
        )


@nexusrpc.handler.service_handler(service=ProcessorService)
class ProcessorServiceHandler:
    def __init__(self, delay_seconds: float) -> None:
        self._delay_seconds = delay_seconds

    async def _start(
        self,
        ctx: nexus.TemporalStartOperationContext,
        client: nexus.TemporalNexusClient,
        operation: str,
        input: ProcessInput,
    ) -> nexus.TemporalOperationResult[ProcessResult]:
        # The request ID is the same for each retry of one start request.
        workflow_id = f"{operation}-{ctx.request_id}"
        logger.info("Starting workflow %s", workflow_id)
        return await client.start_workflow(
            ProcessTextWorkflow.run,
            ProcessRequest(operation, input.text, self._delay_seconds),
            id=workflow_id,
        )

    @nexus.temporal_operation
    async def transform(
        self,
        ctx: nexus.TemporalStartOperationContext,
        client: nexus.TemporalNexusClient,
        input: ProcessInput,
    ) -> nexus.TemporalOperationResult[ProcessResult]:
        return await self._start(ctx, client, "transform", input)

    @nexus.temporal_operation
    async def analyze(
        self,
        ctx: nexus.TemporalStartOperationContext,
        client: nexus.TemporalNexusClient,
        input: ProcessInput,
    ) -> nexus.TemporalOperationResult[ProcessResult]:
        return await self._start(ctx, client, "analyze", input)


async def run(args: argparse.Namespace) -> None:
    client = await Client.connect(args.address, namespace=args.namespace)
    worker = Worker(
        client,
        task_queue=args.task_queue,
        workflows=[ProcessTextWorkflow],
        activities=[process_text],
        nexus_service_handlers=[ProcessorServiceHandler(args.delay_seconds)],
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    async with worker:
        logger.info("Text processor worker is running on task queue %r", args.task_queue)
        await stop.wait()
    logger.info("Text processor worker stopped")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--address", default="localhost:7233")
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--task-queue", default="text-processor")
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=3.0,
        help="Length of the durable timer in each workflow",
    )
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
