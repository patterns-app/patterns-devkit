from collections import OrderedDict
from basis.node.interface import NodeInterface, Table
from basis.node.node import Node


def myf():
    pass


test_node = Node(
    name="testpy",
    node_callable=lambda ctx: ctx,
    interface=NodeInterface(
        inputs=OrderedDict(table_input=Table("table_input")),
        outputs=OrderedDict(table_output=Table("table_output")),
    ),
)
