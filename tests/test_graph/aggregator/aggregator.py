from basis.node.node import node, stream, table, input, output

output_table = table("output_table")
input_stream = stream("input_stream")
input_table = table("input_table")
# from ..random_source_floats.random_source_floats import random_floats

# random_floats = input_stream("random_floats")


@input(random_floats)
@input(input_table)
@output(output_table)
def aggregator():
    records = list(input_stream)
    # input_stream.append_records(records) <-- throws error
    output_table.write(records)


