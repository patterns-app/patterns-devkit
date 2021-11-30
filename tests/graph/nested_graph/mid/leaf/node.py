from basis.node.node import node, InputStream, OutputTable


@node
def leaf(
    leaf_in=InputStream(description="d", schema="S"),
    leaf_out=OutputTable,
):
    pass
