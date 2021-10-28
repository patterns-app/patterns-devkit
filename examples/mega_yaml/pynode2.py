from basis import use_table

returns_table = use_table("stripe_importer.returns")

@nodedef
def mynode():
    returns_table = use_table("returns").as_dataframe()
    # OR as chunks: returns_table = protocol.load_table("returns").as_dataframe_chunks(1000)
    charges_table = use_table("charges").as_records()
    events_stream = use_stream("events").as_iterator()
    # MATH
    ltv_df = []
    churn_df = []
    protocol.store_as_table("ltv", ltv_df)
    for r in churn_df:
        protocol.emit_to_stream("churn_rate", r)
