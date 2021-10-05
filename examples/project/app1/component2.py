import random

from basis import streaming_simple_component


@streaming_simple_component
def component2(input_record):
    input_record["new_value"] = random.random()
    return input_record
