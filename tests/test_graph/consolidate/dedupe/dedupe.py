from basis import node, stream, table, input, output
from ..aggregator.aggregator import aggregated

deduped = stream("deduped")


@input(aggregated)
@output(deduped)
@parameter(parameter("myparam", "int", default=0))
def aggregator():
    df = aggregated.as_dataframe()
    ddf = df.drop_duplicates()
    deduped.write(ddf)
