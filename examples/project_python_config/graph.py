from basis import graph, Port


graph(
    name="python_configs_example",
    input_ports=[Port("customers", proxy_to="node1@customers")],
    output_ports=[Port("ltv_table", proxy_to="node2@ltv_table")],
    nodes=[subgraph1, subgraph2,],
)
