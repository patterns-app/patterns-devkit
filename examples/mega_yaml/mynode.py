from basis import use_table, use_stream, create_table, create_stream, node

# Inputs
returns_table = use_table("stripe_importer.returns")
charges_table = use_table("stripe_importer.charges")
raw_stream = use_stream("webhook_leads")

# Outputs
summary_table = create_table("summary")
enriched_stream = create_stream("enriched_leads")

# Code
@node
def enrich_leads():
    cdf = charges_table.as_dataframe()
    rdf = returns_table.as_dataframe()
    for r in raw_stream:
        ...
    # MATH
    ltv_df = []
    churn_df = []
    summary_table.write("ltv", ltv_df)
    for r in churn_df:
        enriched_stream.append(r)


class EnrichLeads(Node):
    """
    Documentation goes here
    """
    inputs = [
        returns_table,
        charges_table,
        raw_stream,

    ]
    outputs = [
        summary_table,
        enriched_stream,
    ]
    def run(self):
        cdf = charges_table.as_dataframe()
        rdf = returns_table.as_dataframe()
        for r in raw_stream:
            ...
        # MATH
        ltv_df = []
        churn_df = []
        summary_table.write("ltv", ltv_df)
        for r in churn_df:
            enriched_stream.append(r)


