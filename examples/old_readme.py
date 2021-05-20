# flake8: noqa F402
import sys

from snapflow import Environment, datafunction, run, sql_datafunction
from snapflow.core.declarative.base import load_yaml
from snapflow.core.declarative.dataspace import DataspaceCfg
from snapflow.core.declarative.graph import GraphCfg
from snapflow.core.module import DEFAULT_LOCAL_MODULE
from snapflow_stripe import module as stripe

sys.path.append(".")


@datafunction
def customer_lifetime_sales(txs):
    txs_df = txs.as_dataframe()
    return txs_df.groupby("customer")["amount"].sum().reset_index()


@sql_datafunction
def customer_lifetime_sales_sql():
    return "select customer, sum(amount) as amount from txs group by customer"
    # Can use jinja templates too
    # return template("sql/customer_lifetime_sales.sql", ctx)


g = GraphCfg(
    **load_yaml(
        """
nodes:
  - key: stripe_charges
    function: stripe.import_charges
    params:
      api_key: sk_test_4eC39HqLyjWDarjtT1zdp7dc
  - key: accumulated_stripe_charges
    function: core.accumulator
    input: stripe_charges
  - key: stripe_customer_lifetime_sales
    function: customer_lifetime_sales
    input: accumulated_stripe_charges
"""
    )
)

assert len(g.nodes) == 3

ds = DataspaceCfg(graph=g)
env = Environment(dataspace=ds)
# env.add_module(stripe)
run(g, env=env, execution_timelimit_seconds=1)

# Get the final output block
datablock = env.get_latest_output(g.get_node("stripe_customer_lifetime_sales"))
df = datablock.as_dataframe()
assert len(df.columns) == 2
assert len(df) > 1 and len(df) <= 100  # Stripe data varies
