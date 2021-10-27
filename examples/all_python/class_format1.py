from basis import Node, Table, Graph, Stream

from .stripe import StripeImporter


class MyNode(Node):
    """
    Documentation goes here
    """

    summary_table = Table()
    enriched_stream = Stream()

    def run(self):
        df = StripeImporter.returns.as_dataframe()
        df = StripeImporter.charges.as_dataframe()
        for r in WebHookLeads.leads:
            ...
        self.summary_table.write(df)
        for r in df:
            self.enriched_stream.append(r)




