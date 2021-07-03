from __future__ import annotations

from pprint import pprint

import pytest
from basis.core.component import global_library
from basis.core.declarative.base import dump_yaml, load_yaml, update
from basis.core.declarative.dataspace import DataspaceCfg
from basis.core.declarative.flow import FlowCfg
from basis.core.declarative.graph import GraphCfg
from basis.core.flattener import flatten_graph_config
from basis.modules import core
from pydantic.error_wrappers import ValidationError
from tests.utils import (
    function_chain_t1_to_t2,
    function_generic,
    function_multiple_input,
    function_self,
    function_t1_source,
    function_t1_to_t2,
    make_test_env,
)


def make_graph() -> GraphCfg:
    env = make_test_env()
    env.add_module(core)
    return GraphCfg(
        nodes=[
            GraphCfg(key="node1", function="function_t1_source"),
            GraphCfg(key="node2", function="function_t1_source"),
            GraphCfg(key="node3", function="function_t1_to_t2", input="node1"),
            GraphCfg(key="node4", function="function_t1_to_t2", input="node2"),
            GraphCfg(key="node5", function="function_generic", input="node4"),
            GraphCfg(key="node6", function="function_self", input="node4"),
            GraphCfg(
                key="node7",
                function="function_multiple_input",
                inputs={"input": "node4", "other_t2": "node3"},
            ),
        ]
    )


def test_dupe_node():
    # TODO: validate
    g = make_graph()
    with pytest.raises(ValidationError):
        d = GraphCfg(key="node1", function="function_t1_source")
        GraphCfg(nodes=g.nodes + [d])


def test_declared_graph():
    g = make_graph()
    n1 = g.get_node("node1")
    n2 = g.get_node("node2")
    n4 = g.get_node("node4")
    n5 = g.get_node("node5")
    assert len(list(g.nodes)) == 7
    assert g.get_all_upstream_dependencies_in_execution_order(n1) == [n1]
    assert g.get_all_upstream_dependencies_in_execution_order(n5) == [
        n2,
        n4,
        n5,
    ]


def test_make_graph():
    g = make_graph()
    nodes = {
        "node1",
        "node2",
        "node3",
        "node4",
        "node5",
        "node6",
        "node7",
    }
    assert set(n.key for n in g.nodes) == nodes
    n3 = g.get_node("node3")
    n7 = g.get_node("node7")
    assert len(g.get_all_upstream_dependencies_in_execution_order(n3)) == 2
    assert len(g.get_all_upstream_dependencies_in_execution_order(n7)) == 5
    assert len(g.get_all_nodes_in_execution_order()) == len(nodes)
    execution_order = [n.key for n in g.get_all_nodes_in_execution_order()]
    expected_orderings = [
        [
            "node2",
            "node4",
            "node5",
        ],
        [
            "node2",
            "node4",
            "node6",
        ],
        [
            "node1",
            "node3",
            "node7",
        ],
    ]
    # TODO: graph sort not stable!
    for ordering in expected_orderings:
        for i, n in enumerate(ordering[:-1]):
            assert execution_order.index(n) < execution_order.index(ordering[i + 1])


def test_graph_from_yaml():
    g = GraphCfg(
        **load_yaml(
            """
            nodes:
            - key: stripe_charges
              function: stripe.extract_charges
              params:
                api_key: "*****"
            - key: accumulated_stripe_charges
              function: core.accumulator
              input: stripe_charges
            - key: stripe_customer_lifetime_sales
              function: customer_lifetime_sales
              input: accumulated_stripe_charges
            """
        )
    )
    assert len(list(g.nodes)) == 3
    assert g.get_node("stripe_charges").params == {"api_key": "*****"}
    assert g.get_node("accumulated_stripe_charges").get_inputs() == {
        "stdin": "stripe_charges"
    }
    assert g.get_node("stripe_customer_lifetime_sales").get_inputs() == {
        "stdin": "accumulated_stripe_charges"
    }


basic_graph = """
basis:
  initialize_metadata_storage: false
metadata_storage: sqlite://.basis.db
storages:
  - postgres://localhost:5432/basis
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
    #     from_email: automated@basis.ai
    #     to_email: basis-errors@basis.ai
    #     max_records: 100
    """

accum_flow = """
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


def test_resolve_and_flatten():
    # TODO: more complex nested scenarios (see whiteboard snapshot)

    d = load_yaml(accum_flow)
    ad = FlowCfg(**d)
    global_library.add_flow(ad)

    d = load_yaml(basic_graph)
    ds = DataspaceCfg(**d)

    assert set([n.key for n in ds.graph.nodes]) == {"import_csv", "stripe_charges"}
    assert ds.graph.get_node("import_csv").function_cfg is None
    assert not ds.graph.get_node("stripe_charges").nodes
    resolved = ds.resolve()
    assert resolved.graph.get_node("import_csv").function_cfg is not None
    assert len(resolved.graph.get_node("stripe_charges").nodes) == 2

    flat = flatten_graph_config(resolved.graph)
    assert set([n.key for n in flat.nodes]) == {
        "import_csv",
        "stripe_charges.accumulate",
        "stripe_charges.dedupe",
    }
    assert flat.get_node("stripe_charges.accumulate").get_inputs() == {
        "stdin": "import_csv"
    }
    assert flat.get_node("stripe_charges.dedupe").get_inputs() == {
        "stdin": "stripe_charges.accumulate"
    }


def test_augmentations():
    # TODO: more complex nested scenarios (see whiteboard snapshot)
    ad = """
        name: myflow
        namespace: core
        graph:
          nodes:
            - key: step1
              function: core.accumulator_sql
              accumulate: true
              dedupe: true
              alias: alias_step1
            - key: step2
              function: core.dedupe_keep_latest_sql
              input: step1
          stdin_key: step1
          stdout_key: step2
    """
    d = load_yaml(ad)
    ad = FlowCfg(**d)
    global_library.add_flow(ad)

    g = """
        basis:
          initialize_metadata_storage: false
        metadata_storage: sqlite://.basis.db
        storages:
          - postgres://localhost:5432/basis
        namespaces:
          - stripe
        graph:
          nodes:
            - key: import_csv
              function: core.import_local_csv
              params:
                path: "*****"
              accumulate: true
              dedupe: true
              alias: alias_import_csv
            - key: stripe_charges
              flow: myflow
              input: import_csv
              params:
                dedupe: KeepLatestRecord # Default
    """
    d = load_yaml(g)
    ds = DataspaceCfg(**d)

    assert set([n.key for n in ds.graph.nodes]) == {"import_csv", "stripe_charges"}
    assert ds.graph.get_node("import_csv").function_cfg is None
    assert not ds.graph.get_node("stripe_charges").nodes
    resolved = ds.resolve()
    assert resolved.graph.get_node("import_csv").function_cfg is not None
    assert len(resolved.graph.get_node("stripe_charges").nodes) == 2

    flat = flatten_graph_config(resolved.graph)
    # print(dump_yaml(flat.dict()))
    # Test flattening
    assert set([n.key for n in flat.nodes]) == {
        "import_csv.dedupe",
        "import_csv.accumulate",
        "stripe_charges.step1.source",
        "stripe_charges.step2",
        "stripe_charges.step1.dedupe",
        "stripe_charges.step1.accumulate",
        "import_csv.source",
    }
    # Test input key modifications
    assert flat.get_node("stripe_charges.step1.source").get_inputs() == {
        "stdin": "import_csv.dedupe"
    }
    assert flat.get_node("stripe_charges.step2").get_inputs() == {
        "stdin": "stripe_charges.step1.dedupe"
    }
    assert flat.get_node("stripe_charges.step1.accumulate").get_inputs() == {
        "stdin": "stripe_charges.step1.source"
    }
    # Test alias on flattened nodes
    assert flat.get_node("stripe_charges.step1.accumulate").get_inputs() == {
        "stdin": "stripe_charges.step1.source"
    }
    expected = """
key: default
nodes:
- key: import_csv.source
  function: core.import_local_csv
  params:
    path: '*****'
- key: import_csv.accumulate
  function: core.accumulate
  inputs:
    stdin: import_csv.source
- key: import_csv.dedupe
  function: core.dedupe_keep_latest
  inputs:
    stdin: import_csv.accumulate
  alias: alias_import_csv
- key: stripe_charges.step1.source
  function: core.accumulator_sql
  inputs:
    stdin: import_csv.dedupe
- key: stripe_charges.step1.accumulate
  function: core.accumulate
  inputs:
    stdin: stripe_charges.step1.source
- key: stripe_charges.step1.dedupe
  function: core.dedupe_keep_latest
  inputs:
    stdin: stripe_charges.step1.accumulate
  alias: stripe_charges.step1
- key: stripe_charges.step2
  function: core.dedupe_keep_latest_sql
  inputs:
    stdin: stripe_charges.step1.dedupe
  alias: stripe_charges
"""
    # TODO: we lost the alias on the inner step? Is that ok? What is ideal behavior? Nested aliases kinda tricky anyways?
    exp = GraphCfg(**load_yaml(expected))
    assert flat.key == exp.key
    for n in flat.nodes:
        n = update(n, function_cfg=None)
        assert n == exp.get_node(n.key), n.key
