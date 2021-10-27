from basis import BasisProtocol, nodedef


@nodedef("mynode") # <-- this name, or the function name, must match name in config
def mynode(protocol: BasisProtocol):
    input1 = protocol.get_table_input("input1")