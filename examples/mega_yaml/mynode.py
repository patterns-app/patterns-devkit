from basis import BasisProtocol, nodedef


@nodedef
def mynode(protocol: BasisProtocol):
    returns_table = protocol.load_table("returns").as_dataframe()
    # OR as chunks: returns_table = protocol.load_table("returns").as_dataframe_chunks(1000)
    charges_table = protocol.load_table("charges").as_records()
    events_stream = protocol.load_stream_iterator("events")
    # MATH
    ltv_df = []
    churn_df = []
    protocol.store_as_table("ltv", ltv_df)
    for r in churn_df:
        protocol.emit_to_stream("churn_rate", r)
