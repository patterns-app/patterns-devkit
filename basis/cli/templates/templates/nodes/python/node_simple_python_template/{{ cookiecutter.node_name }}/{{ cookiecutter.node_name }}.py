from basis import node, InputStream, OutputTable, Parameter


@node
def {{ cookiecutter.node_name }}(
    input_stream=InputStream,
    output_table=OutputTable,
    myparam=Parameter(type='text'),
):
    # Do things with the records:
    records = list(input_stream.records())

    # Write records to the table:
    output_table.write(records)
