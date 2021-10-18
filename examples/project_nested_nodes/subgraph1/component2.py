import random

from basis import simple_streaming_component


@simple_streaming_component
def component2(input_record):
    input_record["new_value"] = random.random()
    return input_record
