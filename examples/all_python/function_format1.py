from basis import use_table, use_stream, create_table, create_stream, node


# Code
@node
def enrich_leads(ctx):
    cdf = ctx.use_table("").as_dataframe()
    rdf = returns_table.as_dataframe()
    for r in raw_stream:
        ...
    # MATH
    ltv_df = []
    churn_df = []
    summary_table.write("ltv", ltv_df)
    for r in churn_df:
        enriched_stream.append(r)
