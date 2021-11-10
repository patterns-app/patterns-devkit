import random

from basis import table, stream, input, output


random_floats = stream("random_floats")


@output(random_floats)
def random_source_floats():
    records = [{"a": random.random() * 100} for _ in range(100)]
    random_floats.append_records(records)
