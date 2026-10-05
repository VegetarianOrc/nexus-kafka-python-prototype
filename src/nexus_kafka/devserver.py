"""Run a Temporal dev server that supports Nexus handler callbacks.

The released dev server does not support handler callbacks yet. This script
downloads a special build, starts it on fixed ports, and creates the Nexus
endpoint of the processor worker.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal

import temporalio.api.nexus.v1
import temporalio.api.operatorservice.v1
from temporalio.testing import WorkflowEnvironment

logger = logging.getLogger("nexus_kafka.devserver")

DEV_SERVER_VERSION = "v1.9.2-nexus-handler-callbacks"

DYNAMIC_CONFIG = [
    "history.enableChasm=true",
    "history.enableTransitionHistory=true",
    "history.enableCHASMCallbacks=true",
    "nexusoperation.enableStandalone=true",
]


async def _create_endpoint(
    env: WorkflowEnvironment, name: str, namespace: str, task_queue: str
) -> None:
    operator = env.client.operator_service
    existing = await operator.list_nexus_endpoints(
        temporalio.api.operatorservice.v1.ListNexusEndpointsRequest(name=name)
    )
    if existing.endpoints:
        logger.info("Nexus endpoint %r already exists", name)
        return
    await operator.create_nexus_endpoint(
        temporalio.api.operatorservice.v1.CreateNexusEndpointRequest(
            spec=temporalio.api.nexus.v1.EndpointSpec(
                name=name,
                target=temporalio.api.nexus.v1.EndpointTarget(
                    worker=temporalio.api.nexus.v1.EndpointTarget.Worker(
                        namespace=namespace, task_queue=task_queue
                    )
                ),
            )
        )
    )
    logger.info("Created Nexus endpoint %r -> task queue %r", name, task_queue)


async def run(args: argparse.Namespace) -> None:
    extra_args: list[str] = []
    for value in DYNAMIC_CONFIG:
        extra_args += ["--dynamic-config-value", value]
    env = await WorkflowEnvironment.start_local(
        namespace=args.namespace,
        port=args.port,
        ui=True,
        ui_port=args.ui_port,
        dev_server_download_version=DEV_SERVER_VERSION,
        dev_server_database_filename=args.db,
        dev_server_extra_args=extra_args,
    )
    try:
        await _create_endpoint(env, args.endpoint, args.namespace, args.task_queue)
        logger.info(
            "Dev server is running: frontend localhost:%d, UI http://localhost:%d",
            args.port,
            args.ui_port,
        )
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        await stop.wait()
    finally:
        await env.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--port", type=int, default=7233)
    parser.add_argument("--ui-port", type=int, default=8233)
    parser.add_argument("--endpoint", default="text-processor")
    parser.add_argument("--task-queue", default="text-processor")
    parser.add_argument(
        "--db", default=None, help="SQLite file to keep state between runs"
    )
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
