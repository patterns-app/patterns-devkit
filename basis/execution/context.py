from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Iterable, Iterator, List, Optional, Union

from basis.configuration.node import NodeCfg
from basis.node.interface import (
    DEFAULT_ERROR_NAME,
    DEFAULT_STATE_NAME,
    IoBase,
    NodeInterface,
)
from basis.node.node import Node
from commonmodel.base import Schema
from dcp.data_format.base import DataFormat
from dcp.storage.base import Storage


@dataclass(frozen=True)
class Context:
    node: Node
    node_cfg: NodeCfg

    ### Input
    def get_records(self, input_name: str) -> Iterator[Dict]:
        """
        Returns an interator of data records for the given streaming input.

        Caller is responsible for calling `checkpoint` to mark iterated records as
        processed.
        """

    def get_table(self, input_name: str) -> Optional[TableManager]:
        "Returns a TableManager object for the given table input"

    def get_state(self) -> Dict:
        "Returns latest state object as dict"

    def get_state_value(self, key: str, default: Any = None) -> Any:
        "Returns latest state value for given key"

    def checkpoint(self, input_name: str = None):
        """
        Saves progress on given streaming input, marking all iterated records
        as processed.
        """

    def get_raw_records(self, input_name: str) -> Iterator[Dict]:
        """
        Returns an iterator of raw Record objects for the given streaming input.
        These objects include the record id and timestamp. To iterate over actual
        data records, call `get_records`.

        Caller is responsible for calling `checkpoint` to mark iterated records as
        processed.
        """

    def get_node_interface(self) -> NodeInterface:
        "Returns node interface"

    ### Output
    def append_record(
        self,
        output_name: str,
        record: Any,
        schema: Union[str, Schema, None] = None,
    ):
        "Appends single record to given output stream"

    def store_as_table(
        self,
        output_name: str,
        records: Any,
    ):
        "Stores provided data records as table"

    def output_existing_table(
        self,
        output_name: str,
        table_name: str = None,
        storage: Union[str, Storage] = None,
        schema: Union[str, Schema, None] = None,
        data_format: Union[str, DataFormat] = None,
    ):
        "Logs existing table as output from this node"

    def append_records(
        self,
        output_name: str,
        records_obj: Iterable[Any],
        schema: Union[str, Schema, None] = None,
    ):
        "Appends batch of records to given output stream"

    def append_error(self, error_obj: Any, error_msg: str):
        # TODO: create error record
        self.append_record(output_name=DEFAULT_ERROR_NAME, record=error_obj)

    def set_state(self, state: Dict):
        self.store_as_table(output_name=DEFAULT_STATE_NAME, records=[state])

    def set_state_value(self, key: str, value: Any):
        state = self.get_state()
        state[key] = value
        self.set_state(state)

    ### Params
    def get_param(self, key: str, default: Any = None) -> Any:
        if default is None:
            try:
                default = self.node.interface.parameters[key].default
            except KeyError:
                pass
        return self.node_cfg.node_params.get(key, default)

    def get_params(self, defaults: Dict[str, Any] = None) -> Dict[str, Any]:
        # TODO: do this once
        final_params = {
            p.name: p.default for p in self.node.interface.parameters.values()
        }
        final_params.update(defaults or {})
        final_params.update(self.node_cfg.node_params)
        return final_params

    ### Other
    def should_continue(self) -> bool:
        """
        Long running nodes should check this function periodically
        to honor time limits gracefully.
        """
