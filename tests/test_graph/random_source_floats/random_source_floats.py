import random

from basis import table, stream, input, output


random_floats = stream("random_floats", schema=MySchema)
output_table = table("random_floats")
limit = parameter("limit", "int", default=100)


@output(random_floats)
@parameter(limit)
def random_source_floats():
    records = [{"a": random.random() * limit.get_value()} for _ in range(100)]
    for record in random_floats.get_records():
        # Do something
    random_floats.append_records(records)
    df = input_table.as_dataframe()
    df = input_table.as_records()
    df = input_table.as_sql("select * from self")
    output_table.write_dataframe(df)
    output_table.write_records(records)

    state = get_state()
    update_state(new_state)
    error_stream.append_error(myerror)
