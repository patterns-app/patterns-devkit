from typing import Dict, Union

from snapflow.core.declarative.base import update
from snapflow.core.declarative.graph import DedupeBehavior, GraphCfg


def flatten_sub_node(
    cfg: GraphCfg, parent_key: str, parent_stdout_key: Union[str, None]
) -> GraphCfg:
    sub_node_key = parent_key + "." + cfg.key
    new_inputs = {}
    for input_name, node_key in cfg.get_inputs().items():
        new_inputs[input_name] = parent_key + "." + node_key
    alias = None
    if cfg.key == parent_stdout_key:
        alias = parent_key
    return update(cfg, key=sub_node_key, inputs=new_inputs, input=None, alias=alias)


def update_sub_node_inputs(
    sub_node: GraphCfg, parent_key: str, input_name: str, input_node_key: str
) -> GraphCfg:
    """
    "Passes in" parent inputs to appropriate sub node. (For non-stdin scenarios)
    The parent node originally had:
        inputs:
            sub_node_key.path: input1
    So now on 'sub_node_key' node we want to "pass in" this outer input. So sub_node gets:
        inputs:
            path: input1
    """
    sub_input_name = parent_key + "." + input_name
    assert sub_input_name.startswith(sub_node.key)
    existing_sub_inputs = sub_node.get_inputs()
    sub_input_path = sub_input_name[len(sub_node.key + ".") :] or "stdin"
    existing_sub_inputs.update({sub_input_path: input_node_key})
    sub_node = update(sub_node, inputs=existing_sub_inputs, input=None)
    return sub_node


def update_node_inputs(n: GraphCfg, stdout_lookup: Dict[str, str]) -> GraphCfg:
    inputs = n.get_inputs()
    new_inputs = {}
    for name, input_key in inputs.items():
        new_inputs[name] = stdout_lookup.get(input_key, input_key)
    if new_inputs != n.inputs:
        n = update(n, inputs=new_inputs, input=None)
    return n


def handle_augmentations(n: GraphCfg) -> GraphCfg:
    """
    Handle common shortcuts (accumulate, dedupe, and conform), returning
    existing node if none are set, otherwise constructs sub-graph and
    returns that.
    """
    if not n.has_augmentations():
        return n
    nodes = []
    source_node = GraphCfg(
        key="source",
        function=n.function,
        function_cfg=n.function_cfg,
        params=n.params,
    )
    nodes.append(source_node)
    leaf_key = source_node.key
    if n.accumulate:
        # Assign original alias (or key name if no alias) as an alias if this is leaf node
        # alias = n.alias or n.key if not n.dedupe and not n.conform_to_schema else None
        nodes.append(
            GraphCfg(
                key="accumulate",
                function="core.accumulate",
                input=leaf_key,
                # alias=alias,
            )
        )
        leaf_key = "accumulate"
    if n.dedupe:
        # Assign original alias (or key name if no alias) as an alias to this is leaf node
        # alias = n.alias or n.key if not n.conform_to_schema else None
        dedupe = n.dedupe
        if isinstance(dedupe, bool):
            dedupe = DedupeBehavior.LATEST_RECORD
        if dedupe != DedupeBehavior.LATEST_RECORD:
            raise NotImplementedError(dedupe)
        nodes.append(
            GraphCfg(
                key="dedupe",
                function="core.dedupe_keep_latest",
                input=leaf_key,
                # alias=alias,
            )
        )
        leaf_key = "dedupe"
    if n.conform_to_schema:
        raise NotImplementedError(
            f"Conform to schema not implemented yet ({n.conform_to_schema})"
        )
    return update(
        n,
        nodes=nodes,
        function=None,
        function_cfg=None,
        params={},
        stdin_key="source",
        stdout_key=leaf_key,
        stderr_key="source.stderr",
        accumulate=False,
        dedupe=False,
        conform_to_schema=None,
    )


def flatten_graph_config(config: GraphCfg) -> GraphCfg:
    while not config.is_flattened():
        stdout_lookup = {}
        flattened_nodes: Dict[str, GraphCfg] = {}
        parent_nodes = [handle_augmentations(parent) for parent in config.nodes]
        for parent in parent_nodes:
            if parent.nodes:
                if parent.stdout_key:
                    stdout_lookup[parent.key] = parent.key + "." + parent.stdout_key
                for sub_node in parent.nodes:
                    new_sub_node = flatten_sub_node(
                        sub_node, parent.key, parent.stdout_key
                    )
                    flattened_nodes[new_sub_node.key] = new_sub_node
            else:
                flattened_nodes[parent.key] = parent
        for parent in parent_nodes:
            if not parent.nodes:
                continue
            for input_name, input_node_key in parent.get_inputs().items():
                if input_name == "stdin":
                    assert (
                        parent.stdin_key is not None
                    ), "Must specify stdin when multiple nodes"  # TODO: or continue?
                    input_name = parent.stdin_key
                # input_name_node = input_name.split(".")[0]
                # input_name_path = ".".join(input_name.split(".")[1:]) or "stdin"
                new_sub_input_name = parent.key + "." + input_name
                for key, sub_node in flattened_nodes.items():
                    if new_sub_input_name.startswith(key):
                        sub_node = update_sub_node_inputs(
                            sub_node, parent.key, input_name, input_node_key
                        )
                        flattened_nodes[key] = sub_node
                        break
        resolved_nodes = []
        for n in flattened_nodes.values():
            new_node = update_node_inputs(n, stdout_lookup)
            resolved_nodes.append(new_node)
        config = update(config, nodes=resolved_nodes)
    return config
