import random

from basis import table, stream, input, output


random_ints = table("random_ints")


@output(random_ints)
def random_source_ints():
    records = [{"a": random.randint(0, 100)} for _ in range(100)]
    random_ints.append_records(records)
