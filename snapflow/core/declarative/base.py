from __future__ import annotations

from enum import Enum
from typing import (
    Dict,
    TYPE_CHECKING,
    TypeVar,
)
import pydantic
from snapflow.core.component import ComponentLibrary, global_library


class PydanticBase(pydantic.BaseModel):
    class Config:
        extra = "forbid"


class FrozenPydanticBase(PydanticBase):
    class Config:
        frozen = True


F = TypeVar("F", bound=FrozenPydanticBase)


def update(o: F, **kwargs) -> F:
    d = o.dict()
    d.update(**kwargs)
    return type(o)(**d)


def load_yaml(yml: str) -> Dict:
    try:
        from yaml import CLoader as Loader, CDumper as Dumper
    except ImportError:
        from yaml import Loader, Dumper
    return yaml.load(yml, Loader=Loader)


if __name__ == "__main__":
    import yaml

    try:
        from yaml import CLoader as Loader, CDumper as Dumper
    except ImportError:
        from yaml import Loader, Dumper
    from snapflow.core.declarative.flow import FlowCfg
    from snapflow.core.declarative.dataspace import DataspaceCfg

    ad = """
  name: accumulate_and_dedupe_sql
  namespace: core
  graph:
    nodes:
      - key: accumulate
        function: core.accumulator_sql
      - key: dedupe
        function: core.dedupe_keep_latest_sql
        input: accumulate
    stdout_key: dedupe
    stdin_key: accumulate
    """
    d = yaml.load(ad, Loader=Loader)
    ad = FlowCfg(**d)

    g = """
snapflow:
  initialize_metadata_storage: false
metadata_storage: sqlite://.snapflow.db
storages:
  - postgres://localhost:5432/snapflow
namespaces:
  - stripe
graph:
  nodes:
    - key: import_csv
      function: core.import_local_csv
      params:
        path: "*****"
    - key: stripe_charges
      flow: core.accumulate_and_dedupe_sql
      input: import_csv
      params:
        dedupe: KeepLatestRecord # Default
    # - key: email_errors
    #   function: core.email_records
    #   input: import_csv.stderr
    #   params:
    #     from_email: automated@snapflow.ai
    #     to_email: snapflow-errors@snapflow.ai
    #     max_records: 100
    """
    d = yaml.load(g, Loader=Loader)
    p = DataspaceCfg(**d)
    from snapflow.modules import core

    from pprint import pprint

    pprint(p.dict())
    # print(global_library.namespace_precedence)
    # print(global_library.functions)
    global_library.add_flow(ad)
    r = p.resolve(global_library)
    print("######## RESOLVED")
    pprint(r.dict())
    from snapflow.core.flattener import flatten_graph_config

    flat = flatten_graph_config(r.graph)
    print("######## FLAT")
    pprint(flat.dict())
