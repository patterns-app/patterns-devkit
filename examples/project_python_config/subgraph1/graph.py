from basis import graph, Port, sql_node
from . import node1, node3, nestedgraph1


node2 = sql_node("node2.sql", __file__)


graph(
    name="subgraph1",
    # By default exposes all sub node ports
    # input_ports=[Port("customers", proxy_to="node1@customers")],
    # output_ports=[Port("ltv_table", proxy_to="node2@ltv_table")],
    nodes=[node1, node2, node3, nestedgraph1],
)
