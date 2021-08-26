from __future__ import annotations
from basis.core.declarative.execution import ExecutableCfg, ExecutionResult

from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Union

import requests

from basis.core.environment import Environment

from dcp.utils.common import to_json


@dataclass
class MetadataExecutionResultHandler:
    env: Environment

    def __call__(self, exe: ExecutableCfg, result: ExecutionResult):
        from basis.core.execution.run import handle_execution_result

        with self.env.md_api.begin():
            handle_execution_result(self.env, exe, result)


# Used for local python runtime
global_metadata_result_handler: Optional[MetadataExecutionResultHandler] = None


def get_global_metadata_result_handler() -> Optional[MetadataExecutionResultHandler]:
    return global_metadata_result_handler


def set_global_metadata_result_handler(handler: MetadataExecutionResultHandler):
    global global_metadata_result_handler
    global_metadata_result_handler = handler


@dataclass
class DebugMetadataExecutionResultHandler:
    def __call__(self, exe: ExecutableCfg, result: ExecutionResult):
        print(result.dict())


@dataclass
class RemoteCallbackMetadataExecutionResultHandler:
    callback_url: str
    headers: Optional[Dict] = None

    def __call__(self, exe: ExecutableCfg, result: ExecutionResult):
        headers = {"Content-Type": "application/json"}
        headers.update(self.headers or {})
        data = {"executable": exe.dict(), "result": result.dict()}
        requests.post(self.callback_url, data=to_json(data), headers=headers)
