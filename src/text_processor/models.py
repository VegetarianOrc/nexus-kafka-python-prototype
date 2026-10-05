"""Nexus service contract of the processor worker."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import nexusrpc


@dataclass
class ProcessInput:
    text: str


@dataclass
class ProcessResult:
    operation: str
    workflow_id: str
    output: dict[str, Any]


@nexusrpc.service(name="text-processor")
class ProcessorService:
    transform: nexusrpc.Operation[ProcessInput, ProcessResult]
    analyze: nexusrpc.Operation[ProcessInput, ProcessResult]
