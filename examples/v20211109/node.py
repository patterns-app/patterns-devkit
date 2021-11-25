from basis import (
    use_table,
    use_stream,
    create_table,
    create_stream,
    node,
    Table,
    table,
    output,
    input,
)

# Node imports
from othernode import input_stream1

# Outputs
my_output = table("output_table2")

# Parameters
my_param = parameter("mparam", type="int")

# Node definition
@input(input_stream1)
@output(my_output)
def mynode():
    df = input_stream1.read_dataframe()
    my_output.write_dataframe(df)
    for record in input_stream1:
        my_out_stream.append_record(record)