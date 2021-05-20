# flake8: noqa F402
import importlib
import os
import sys
import tempfile
from pathlib import Path

from cleo import CommandTester
from snapflow import Environment, datafunction, run, sql_datafunction
from snapflow.cli.app import app
from snapflow.core.declarative.base import load_yaml
from snapflow.core.declarative.dataspace import DataspaceCfg
from snapflow.core.declarative.graph import GraphCfg
from snapflow.core.module import DEFAULT_LOCAL_MODULE
from snapflow_stripe import module as stripe

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
from snapflow import datafunction, DataBlock


@datafunction
def customer_lifetime_sales(txs: DataBlock[Transaction]) -> DataFrame:
    txs_df = txs.as_dataframe()
    return txs_df.groupby("customer")["amount"].sum().reset_index()
"""
with open(pth, "w") as f:
    f.write(fn)

ds = """
storages:
  - sqlite://snapflow_demo.db
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
with open(Path(dirpath) / "snapflow.yml", "w") as f:
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
# datablock = env.get_latest_output(g.get_node("stripe_customer_lifetime_sales"))
# df = datablock.as_dataframe()
# assert len(df.columns) == 2
# assert len(df) > 1 and len(df) <= 100  # Stripe data varies
