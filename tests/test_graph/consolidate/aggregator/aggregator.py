from basis.node.node import node, stream, table, input, output

aggregated = table("aggregated")
input_stream = stream("input_stream")


@input(input_stream)
@output(aggregated)
def aggregator():
    records = list(input_stream)
    aggregated.write(records)
