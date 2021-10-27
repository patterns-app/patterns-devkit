from basis import BasisProtocol, nodedef, input_table, output_table, input_stream, output_stream, parameter


quick_schema = {
    "fields":....
}

class EnrichLeads(Node):
    # Inputs
    returns_table = input_table("returns")
    charges_table = input_table("charges")
    raw_stream = input_stream("leads")

    # Outputs
    summary_table = output_table("summary", schema=MySchema | quick_schema | "common.Transaction")
    enriched_stream = output_stream("enriched_leads")

    # Parameters
    my_param = parameter("my_param")

    def run(self):
        cdf = self.charges_table.as_dataframe()
        rdf = self.returns_table.as_dataframe()
        for r in self.raw_stream:
            ...
        # MATH
        ltv_df = []
        churn_df = []
        self.summary_table.write("ltv", ltv_df)
        for r in churn_df:
            self.enriched_stream.append(r)


