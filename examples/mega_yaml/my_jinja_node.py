from basis import BasisProtocol, nodedef


@nodedef
def my_simple_node(protocol: BasisProtocol):
    leads_table = protocol.get_table_input("leads")
    charges_table = protocol.get_table_input("charges")
    events_stream = protocol.get_stream_input("events")
