# flake8: noqa F402
import os
import sys
import tempfile
from pathlib import Path

from basis.cli.app import app
from basis_modules.modules import stripe
from cleo import CommandTester

sys.path.append(".")


dirpath = tempfile.mkdtemp()
os.chdir(dirpath)

cmd = app.find("new")
cmd_tester = CommandTester(cmd)
cmd_tester.execute("dataspace quickstart")
cmd_tester.execute("function customer_lifetime_sales")

pth = (
    Path(dirpath)
    / "quickstart/functions/customer_lifetime_sales/customer_lifetime_sales.py"
)
print(pth)
fn = """
from __future__ import annotations
from pandas import DataFrame
from basis import function, Block


@function
def customer_lifetime_sales(txs: Block) -> DataFrame:
    txs_df = txs.as_dataframe()
    return txs_df.groupby("customer")["amount"].sum().reset_index()
"""
with open(pth, "w") as f:
    f.write(fn)

ds = (
    """
storages:
  - sqlite:///%s/.basis.db
modules:
  - basis_modules.modules.stripe
graph:
  nodes:
    - key: stripe_charges
      function: stripe.import_charges
      params:
        api_key: sk_test_4eC39HqLyjWDarjtT1zdp7dc
    - key: stripe_customer_lifetime_sales
      function: customer_lifetime_sales
      input: stripe_charges
"""
    % dirpath
)
with open(Path(dirpath) / "basis.yml", "w") as f:
    f.write(ds)

cmd = app.find("run")
cmd_tester = CommandTester(cmd)
cmd_tester.execute("--timelimit=5")

cmd = app.find("output")
cmd_tester = CommandTester(cmd)
cmd_tester.execute("stripe_customer_lifetime_sales")
out = cmd_tester.io.fetch_output()
# TODO: test output

# Get the final output block
# block = env.get_latest_output(g.get_node("stripe_customer_lifetime_sales"))
# df = block.as_dataframe()
# assert len(df.columns) == 2
# assert len(df) > 1 and len(df) <= 100  # Stripe data varies
