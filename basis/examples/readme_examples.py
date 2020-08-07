from basis import Environment


def getting_started_example(env: Environment):
    # TODO: MockEnvironment that doesn't lookup URIs or run anything, but validates basic structure
    env.add_external_source_node(
        name="stripe_txs",
        external_source="stripe.StripeTransactionsResource",
        config={"api_key": "xxxxxxxx"},
    )
    env.add_node(
        name="ltv_model", pipe="bi.TransactionLTVModel", upstream="stripe_txs",
    )
    env.update_all()
