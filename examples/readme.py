# flake8: noqa F402
import sys

import snapflow_stripe as stripe
from snapflow import Environment, Snap, SqlSnap, graph_from_yaml, run
from snapflow.core.module import DEFAULT_LOCAL_MODULE

sys.path.append(".")


@Snap
def customer_lifetime_sales(txs):
    txs_df = txs.as_dataframe()
    return txs_df.groupby("customer")["amount"].sum().reset_index()


@SqlSnap
def customer_lifetime_sales_sql():
    return "select customer, sum(amount) as amount from txs group by customer"
    # Can use jinja templates too
    # return template("sql/customer_lifetime_sales.sql", ctx)


g = graph_from_yaml(
    """
nodes:
  - key: stripe_charges
    snap: stripe.extract_charges
    params:
      api_key: sk_test_4eC39HqLyjWDarjtT1zdp7dc
  - key: accumulated_stripe_charges
    snap: core.dataframe_accumulator
    input: stripe_charges
  - key: stripe_customer_lifetime_sales
    snap: customer_lifetime_sales
    input: accumulated_stripe_charges
"""
)

# print(g)
assert len(g._nodes) == 3


env = Environment(modules=[stripe])
run(g, env=env, node_timelimit_seconds=1)

# Get the final output block
datablock = env.get_latest_output("stripe_customer_lifetime_sales", g)
df = datablock.as_dataframe()
assert len(df.columns) == 2
assert len(df) > 1 and len(df) <= 100  # Stripe data varies
