from snapflow import Environment, graph, node


def getting_started_example(env: Environment):
    # Build the graph
    g = graph()
    stripe_node = g.create_node(
        key="stripe_txs",
        pipe="stripe.extract_charges",
        config={"api_key": "xxxxxxxx"},
    )
    ltv_node = g.create_node(
        key="ltv_model",
        pipe="bi.transaction_ltv_model",
    )
    ltv_node.set_upstream(stripe_node)

    # Run
    env = Environment("sqlite:///snapflow.db")
    print(env.produce(ltv_node).as_dataframe())
