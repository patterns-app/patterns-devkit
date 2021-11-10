from basis.node.node import node, stream, table, input, output

output_table = table("output_table")


@input(input_stream)
@output(output_table)
def aggregator():
    records = list(input_stream)
    output_table.write(records)
