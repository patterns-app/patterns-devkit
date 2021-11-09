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
from othernode import input_stream1


my_output = table("output_table2")
my_param = parameter("mparam", type="int")


@input(input_stream1)
@output(my_output)
def mynode(ctx):
    df = my_input2.read_dataframe()
    my_output.write_dataframe(df)
    for record in my_stream:
        my_out_stream.append_record(record)


