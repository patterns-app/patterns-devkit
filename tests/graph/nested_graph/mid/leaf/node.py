from basis.node.node import node, InputStream, OutputTable


@node
def node(
    node_in=InputStream(description="d", schema="S"),
    node_out=OutputTable,
):
    pass
