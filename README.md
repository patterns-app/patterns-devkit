![basis](https://github.com/kvh/basis/workflows/basis/badge.svg)

<p>&nbsp;</p>
<p align="center">
  <img width="800" src="assets/linear-basis.svg">
  <img width="650" src="assets/basis.svg">
</p>
<h3 align="center">Serverless Data Science</h3>
<p>&nbsp;</p>

**Basis** is the one tool you need to power your entire data science pipeline. At its core, Basis is
a framework for building **functional reactive data flow** from modular
components. It lets developers write pure `datafunctions` in **Python or SQL**
that operate reactively on `datablocks`, immutable sets of data records whose
structure are described by flexible `schemas`.
These functions can be composed into simple or complex data
flows that power all operations of a data system, including:

- API ingestion
- SQL transformation
- Machine learning training and prediction
- Webhooks, event streams, and triggers
- Visualization
- Export and callbacks

The functional reactive paradigm enables powerful benefits:

- **Declarative data flows** — Trade the tangled, stateful messes of traditional ETLs
  for clean declarative flows of data. Mix full rebuilds and incremental updates
  easily and safely.

- **Reusable components** — `datafunctions` and `flows` -- pre-built cloneable graphs -- can be easily plugged together, shared and
  reused across projects.

- **Operational data** - Basis isn't just an ETL tool, its reactive nature and generic abstractions enable developers
  to build operational data flows that put their data to work.

- **Total reproducibility** — Every data record at every pipeline step is preserved in basis,
  along with the code that produced it and the inputs it used. Enables auditing, debugging, and reproducing pipelines.

- **Portability** — Modular and testable functions means it is easy and safe to
  run the same data operation on many major database vendors (postgres, mysql, snowflake, bigquery, redshift),
  and file systems (local, S3, etc), or data format, whether it's csv, json, or apache arrow.

- **Testability** — Data functions provide explicit test
  inputs and the expected output under various data scenarios — a **data pipeline unit test**.

- **High performance** — Datablock immutability means basis can
  optimize data storage operations. It uses [dcp](https://github.com/kvh/dcp) under the hood
  to handle transfer and conversion.

Basis brings the best practices learned over the last 60 years in software to the world of data,
with the goal of global collaboration, reproducible byte-perfect results, and performance at any
scale from laptop to AWS cluster.

> 🚨️ &nbsp; basis is **ALPHA** software. Expect breaking changes to core APIs. &nbsp; 🚨️

## Quick start

Install core library and the Stripe module:

`pip install basis-core basis-modules` or `poetry add basis-core basis-modules`

Start a new dataspace named 'quickstart':

`basis new dataspace quickstart`

Create our own custom data function:

`basis new function customer_lifetime_sales`

Edit `quickstart/functions/customer_lifetime_sales/customer_lifetime_sales.py`:

```python
from __future__ import annotations
from pandas import DataFrame
from basis import datafunction, DataBlock


@datafunction
def customer_lifetime_sales(txs: DataBlock) -> DataFrame:
    txs_df = txs.as_dataframe()
    return txs_df.groupby("customer")["amount"].sum().reset_index()
```

Next, we specify our graph, leveraging the existing
`import_charges` function of the `basis-stripe` module.

Edit `basis.yml`:

```yaml
storages:
  - key: sqlite
    url: sqlite:///.basis_demo.db
runtimes:
  - key: gcf_medium
    url:
default_storage: sqlite
default_runtime: gcf_medium
graph:
  nodes:
    - key: import_stripe
      function: basis_modules.stripe.import
      params:
        endpoints: [charges, invoices, subscriptions, subscription_items]
      params:
        api_key: sk_test_4eC39HqLyjWDarjtT1zdp7dc
    - key: stripe_customer_lifetime_sales
      function: customer_lifetime_sales
      input: import_stripe#charges
```

Now run the dataspace (give a run time limit of 5 seconds for demo purposes):

`basis run --timelimit=5`

And preview the output:

`basis output stripe_customer_lifetime_sales`

## Architecture overview

All basis pipelines are directed graphs of `datafunction` nodes, consisting of one or more "source" nodes
that create and emit datablocks when run. This stream of blocks is
then consumed by downstream nodes, which each in turn may emit their own blocks. Source nodes can be scheduled
to run as needed, downstream nodes will automatically ingest upstream datablocks reactively.

<!-- ![Architecture](assets/architecture.svg) -->

Below are more details on the key components of basis.

### Datablocks

A `datablock` is an immutable set of data records of uniform `schema` -- think csv file, pandas
dataframe, or database table. `datablocks` are the basic data unit of basis, the unit that `datafunctions` take
as input and produce as output. Once created, a datablock's data will never change: no records will
be added or deleted, or data points modified. More precisely, `datablocks` are a reference to an
abstract ideal of a set of records, and will have one or more `StoredDataBlocks` persisting those
records on a specific `storage` medium in a specific `dataformat` -- a CSV on the
local file, a JSON string in memory, or a table in Postgres. Basis abstracts over specific
formats and storage engines, and provides conversion and i/o between them while
maintaining byte-perfect consistency -- to the extent possible for given formats and storages.

### DataFunctions

Data `datafunctions` are the core computational unit of basis. They are pure functions that operate on
`datablocks` and are added as nodes to a function graph, linking one node's output to another's
input via `streams`. DataFunctions are written in python or sql.

DataFunctions may consume (or "reference") zero or more inputs, and may emit zero or more output streams.
DataFunctions can also take parameters. Inputs (upstream nodes) and parameters (configuration)
are inferred automatically from a function's type annotations --
the type of input, whether required or optional, and what schemas/types are expected.

Taking our example from above, we can more explicitly annotate and parameterize it (if there are
no annotations provided on a data function, defaults are assumed). We do this for both python and
sql:

```python
@datafunction
def customer_lifetime_sales(
  ctx: DataFunctionContext,  # Inject a context object
  txs: DataBlock[Transaction],  # Require an input stream conforming to schema "Transaction"
  metric: str = "amount" # Accept an optional string parameter, with default of "amount"
):
    # DataFunctionContext object automatically injected if declared
    txs_df = txs.as_dataframe()
    return txs_df.groupby("customer")[metric].sum().reset_index()

@sql_datafunction
def customer_lifetime_sales_sql():
  return """
      select
          customer
        , sum(:metric) as :metric:raw=amount
      from txs:Transaction
      group by customer
  """
```

Note the special syntax `:metric` in the SQL query for specifying a parameter. It is of type
`raw` since it is used as an identifier (we don't want it quoted as a string), and has a default
value of `amount`, the same as in our python example above.

### Schemas

`Schemas` are record type definitions (think database table schema) that let `datafunctions` specify the
data structure they expect and allow them to inter-operate safely. They also
provide a natural place for field descriptions, validation logic, uniqueness constraints,
default deduplication behavior, relations to other schemas, and other metadata associated with
a specific type of data record.

`Schemas` behave like _interfaces_ in other typed languages. The basis type system is structurally and
gradually typed -- schemas are both optional and inferred, there is no explicit type hierarchy, and
type compatibility can be inspected at runtime. A type is a subtype of, or "compatible" with, another
type if it defines a superset of compatible fields or if it provides an `implementation`
of that type.

A minimal `schema` example, in yaml:

```yaml
name: Order
description: An example schema representing a basic order (purchase)
version: 1.0
unique_on:
  - id
fields:
  id:
    type: Text
    validators:
      - NotNull
  amount:
    type: Decimal(12, 2)
    description: The amount of the transaction
    validators:
      - NotNull
  date:
    type: DateTime
    validators:
      - NotNull
  customer_id:
    type: Text
```

`datafunctions` can declare the `schemas` they expect with type hints, allowing them to specify the
(minimal) contract of their interfaces. Type annotating our earlier examples would look like this:

```python
# In python, use python's type annotations to specify expected schemas:
def sales(txs: DataBlock[Transaction]) -> DataFrame[CustomerMetric]:
    df = txs.as_dataframe()
    return df.groupby("customer_id").sum("amount").reset_index()
```

In SQL, we add type hints with comments after the `select` statement (for output) and after table
identifiers (for inputs):

```sql
select:CustomerMetric
    customer_id
  , sum(amount) as amount
from txs:Transaction
group by customer_id
```

We could also specify `relations` and `implementations` of this type with other types:

```yaml
relations:
  customer:
    type: Customer
    fields:
      id: customer_id
implementations:
  common.TimeSeries:
    datetime: date
    value: amount
```

Here we have _implemented_ the `common.TimeSeries` schema, so any `datafunction` that accepts
timeseries data -- a seasonality modeling function, for example -- can now be applied to
this `Order` data as well. We could also apply this schema implementation ad-hoc at
node declaration time with the `schema_translation` kwarg:

```python
orders = node(order_source)
n = node(
   "seasonality",
   seasonality,
   input=orders,
   schema_translation={
       "date": "datetime",
       "amount": "value",
   })
```

Typing is always optional, our original function definitions were valid with
no annotated `schemas`. Basis `schemas` are a powerful mechanism for producing reusable
components and building maintainable large-scale data projects and ecosystems. They are always
optional though, and should be used when the utility they provide out-weighs any friction.

## Other concepts

### Consistency and Immutability

To the extent possible, basis maintains the same data and byte representation of `datablock`
records across formats and storages. Not all formats and storages support all data representations,
though -- for instance, datetime support differs
significantly across common data formats, runtimes, and storage engines. When it notices a
conversion or storage operation may produce data loss or corruption, basis will try to emit a
warning or, if serious enough, fail with an error. (Partially implemented, see #24)

### Environment and metadata

A basis `environment` tracks the function graph, and acts as a registry for the `modules`,
`runtimes`, and `storages` available to functions. It is associated one-to-one with a single
`metadata database`. The primary responsibility of the metadata database is to track which
nodes have processed which DataBlocks, and the state of nodes. In this sense, the environment and
its associated metadata database contain all the "state" of a basis project. If you delete the
metadata database, you will have effectively "reset" your basis project. (You will
NOT have deleted any actual data produced by the pipeline, though it will be "orphaned".)

### Component Development

Developing new basis components is straightforward and can be done as part of a basis
`module` or as a standalone component. Module development guide and tools coming soon.

### Type system details

Data blocks have three associated schemas:

- Inferred schema - the structure and datatypes automatically inferred from the actual data
- Nominal schema - the schema that was declared (or resolved, for a generic) in the function graph
- Realized schema - the schema that was ultimately used to physically store the data on a specific
  storage (the schema used to create a database table, for instance)

The realized schema is determined by the following factors:

- The setting of `CAST_TO_SCHEMA_LEVEL` to one of `hard`, `soft`, or `none`
- The setting of `FAIL_ON_DOWNCAST` and `WARN_ON_DOWNCAST`
- The discrepancies, if any, between the inferred schema and the nominal schema

The following table gives the logic for possible behavior of realized schema:

|                                                                                                         | none                                      | soft (default)                                                                                                                                            | hard                                                       |
| ------------------------------------------------------------------------------------------------------- | ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| inferred has superset of nominal fields                                                                 | realized is always equivalent to inferred | realized schema == nominal schema, but use inferred field definitions for extra fields.                                                                   | realized equivalent to nominal, extra fields are discarded |
| inferred is missing nullable fields from nominal                                                        | " "                                       | realized schema == nominal schema, but use inferred field definitions for extra fields, fill missing columns with NULL                                    | exception is raised                                        |
| inferred is missing non-nullable fields from nominal                                                    | " "                                       | exception is raised                                                                                                                                       | exception is raised                                        |
| inferred field has datatype mismatch with nominal field definition (eg string in a nominal float field) | " "                                       | realized schema is downcast to inferred datatype (and warning issued if `WARN_ON_DOWNCAST`). If `FAIL_ON_DOWNCAST` is set, an exception is raised instead | exception is raised                                        |
