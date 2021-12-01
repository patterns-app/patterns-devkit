from basis.node.node import node, InputStream, OutputStream


@node
def passthrough(
    pass_in=InputStream, pass_out=OutputStream,
):
    pass
