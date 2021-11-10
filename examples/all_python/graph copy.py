from basis import Graph
from . import random_source, random_source_floats, aggregator


graph = Graph()

n_ints = graph.add_node(random_source)
n_floats = graph.add_node(random_source_floats)
n_agg_ints = graph.add_node(aggregator)
n_agg_floats = graph.add_node(aggregator)

n_agg_ints.set_input(input_stream=n_ints)
n_agg_floats.set_input(input_stream=n_floats)

n_agg_ints.

graph.add_table("random_ints")
