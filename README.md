![snapflow](https://github.com/kvh/snapflow/workflows/snapflow/badge.svg)

<p align="center">
  <img width="80" src="assets/bolt.png">
</p>
<p align="center">
  <img width="500" src="assets/snapflow.svg">
</p>
<h3 align="center">Modern Data Pipelines</h3>
<p>&nbsp;</p>

**Snapflow** is a framework for building end-to-end **functional data pipelines** from modular
components. Snapflow abstracts over underlying database, runtime, and storage resources with
functional type-aware data graphs that operate on streams of **immutable** `datablocks`. These graphs are
composed of discrete `pipes` written in **python or SQL** and connect to
form end-to-end data pipelines, from API extraction to SQL transformation to analysis, modeling, and
visualization.

Snapflow `pipes` optionally expose type interfaces, called `schemas`, that describe the structure,
data types, and semantics of the expected input and output data. These type interfaces allow the
snapflow community to build an ecosystem of interoperable components that power instant "plug and
play" pipelines.

Snapflow brings the best practices learned over the last 60 years in software to the world of data,
with the goal of global collaboration, reproducible byte-perfect results, and performance at any
scale from laptop to AWS cluster.

### Features:

- **Reusable components** — Hundreds of `pipes` and `schemas` ready to
  plug into pipelines in the snapflow repository [Coming soon]. Some examples:

  - [Stripe](https://github.com/kvh/snapflow-stripe.git) module
  - [Shopify](https://github.com/kvh/snapflow-shopify.git) module
  - [Stocks](https://github.com/kvh/snapflow-stocks.git) module
  - [BI](https://github.com/kvh/snapflow-bi.git) (Business Intelligence) module
  - [FRED](https://github.com/kvh/snapflow-fred.git) (Federal Reserve Economic Data) module

- **Testable** — Modular `pipes` allow individual steps in a data process to be
  independently tested and QA'd with the same rigor as software.

- **Flexible** — snapflow lets you build data flows on and across any database or file system.
  It works with big or small data, in both batch and streaming modes.

- **Zero cost abstractions and high performance** — snapflow makes its type and immutability
  guarantees at the abstraction level, so those guarantees can be compiled away at execution time
  for high performance. Developers and analysts can work with clean mental models and strong
  guarantees without incurring performance costs at runtime.

🚨️ &nbsp; snapflow is **ALPHA** software. Expect breaking changes to core APIs. &nbsp; 🚨️

### Example

`pip install snapflow snapflow-stripe snapflow-bi`

```python
from snapflow import graph, produce, operators
import snapflow_stripe as stripe

g = graph()

# Fetch Stripe charges from API:
stripe_source_node = g.node(
    stripe.pipes.extract_charges,
    config={"api_key": "xxxxxxxxxxxx"},
)

# Accumulate all charges:
stripe_charges_node = g.node("core.dataframe_accumulator")
stripe_charges_node.set_upstream(stripe_source_node)

# Define custom pipe:
def customer_lifetime_sales(block):
    df = block.as_dataframe()
    return df.groupby("customer")["amount"].sum().reset_index()

# Add node and take latest accumulated output as input:
lifetime_sales = g.node(customer_lifetime_sales)
lifetime_sales.set_upstream(operators.latest(stripe_charges_node))

# Run:
output = produce(lifetime_sales, modules=[stripe])
print(output.as_dataframe()
```

## Architecture overview

All snapflow pipelines are directed graphs of nodes, consisting of one or more "source" nodes
that emit or create datablocks every time they are run. This stream of blocks is
then be consumed by downstream nodes, each in turn may emit their own blocks, and so on. Nodes
can be run in any order, any number of times. Each time, they consume any new blocks
from upstream until there are none left unprocessed, or they are requested to stop.

![Architecture](assets/architecture.svg)

Below are more details on the key components of snapflow.

### Datablocks

A `datablock` is an immutable set of data records of uniform `schema` -- think csv file
or database table. `datablocks` are the basic data unit of snapflow, the unit that `pipes` take
as input and produce as output. Once created, a datablock's data will never change: no records will
be added or deleted or data points modified. More precisely, `datablocks` are a reference to an
abstract ideal of a set of records, and will have one or more `StoredDataBlocks` persisting those
records on a specific `storage` mediums in a specific `dataformat` -- a CSV on the
local file, a JSON string in memory, or a table in Postgres. Snapflow abstracts over specific
formats and storage engines, and provides conversion and i/o between them while doing its best
to maintain byte-perfect consistency -- to the extent possible for a given format and storage.

### Pipes

`pipes` are the core computational unit of snapflow. They are functions that operate on
`datablocks` and are added as nodes to a pipe graph, linking one node's output to another's
input via `streams`. Pipes are written in python or sql. Below are two equivalent example pipes:

```python
def sales(txs: DataBlock) -> DataBlock:
    df = txs.as_dataframe()
    return df.groupby("customer_id").sum("amount").reset_index()
```

```sql
select
    customer_id
  , sum(amount) as amount
from txs
group by customer_id
```

### Schemas

`schemas` are record type definitions (think database table schema) that let `pipes` specify the
data structure they expect and allow them to inter-operate safely. They also
provide a natural place for field descriptions, validation logic, deduplication
behavior, and other metadata associated with a specific type of data record.

`schemas` behave like _interfaces_ in other languages. The snapflow type system is structurally and
gradually typed -- types are both optional and inferred, there is no explicit type hierarchy, and
type compatibility can be inspected at runtime. A type is subtype of, or "compatible" with, another
type if it defines a superset of compatible fields or if it provides an `implementation`
of that type.

A minimal type example, in yaml:

```yaml
name: Order
description: Generic schema representing a basic order
version: 1.0
unique on: id
on duplicate: KeepFirst
fields:
  id:
    type: Unicode(256)
    validators:
      - NotNull
  amount:
    type: Numeric(12,2)
    description: The amount of the transaction
    validators:
      - NotNull
  date:
    type: DateTime
    validators:
      - NotNull
  customer_id:
    type: Unicode(256)
```

We could also specify `relationships` and `implementations` of this type with other types:

```yaml
relationships:
  customer:
    type: Customer
    fields:
      id: customer_id
implementations:
  common.TimeSeries:
    datetime: date
    value: amount
```

Here we have _implemented_ the `common.TimeSeries` schema, so any `pipe` that accepts
timeseries data, say a seasonality modeling pipe, can now be applied to this `Order` data. We could
also apply this schema implementation ad-hoc at node declaration time with the
`schema_translation` arg:

```python
orders = node(order_source)
n = node(
   "seasonality",
   seasonality,
   upstream=orders,
   schema_translation={
       "datetime": "date",
       "value": "amount",
   })
```

`pipes` can declare the `schemas` they expect with type hints, allowing them to specify the
(minimal) contract of their interfaces. Type annotating our earlier examples would look like this:

```python
# In python, use python's type annotations to specify expected schemas:
@pipe
def sales(txs: DataBlock[Transaction]) -> DataBlock[CustomerMetric]:
    df = txs.as_dataframe()
    return df.groupby("customer_id").sum("amount").reset_index()
```

In SQL, we add type hints with comments after the `select` statement (for output) and after table
identifiers (for inputs):

```sql
-- In SQL, we use comments to specify expected schemas
select -- :CustomerMetric
    customer_id
  , sum(amount) as amount
from txs -- :Transaction
group by customer_id
```

Typing is always optional, our original pipe definitions were valid with
no annotated `schemas`. Snapflow `schemas` are a powerful mechanism for producing reusable
components and building maintainable large-scale data projects and ecosystems. They are always
optional though, and should be used when the utility they provide out-weighs the friction they
introduce.

### Streams

Datablock `streams` connect nodes in the pipe graph. By default every node's output is a simple
stream of datablocks, consumed by one or more other downstream nodes. Stream **operators** allow
you to manipulate these streams:

```python
from snapflow import node
from snapflow.operators import merge, filter

n1 = node(source1)
n2 = node(source2)
combined = merge(n1, n2)
big_blocks_only = filter(combined, function=lambda block: block.count() > 1000)
n3 = node(do_something, upstream=big_blocks_only)
```

Common operators include `latest`, `merge`, `filter`. It's simple to create your own:

```python
@operator
def sample(stream: Stream, sample_rate: float = .5) -> Stream:
    for block in stream:
        if random.random() < sample_rate:
            yield block
```

It's important to note that streams, unlike pipes, never _create_ new datablocks or have any
effect on what is stored on disk. They only alter _which_ datablocks end up as input to a node.

## Other concepts

### Consistency and Immutability

To the extent possible, snapflow maintains the same data and byte representation of `datablock`
records across formats and storages. Not all formats and storages support all data representations,
though -- for instance, empty / null / None / NA support differs
significantly across common data formats, runtimes, and storage engines. When it notices a
conversion or storage operation may produce data loss or corruption, snapflow will try to emit a
warning or, if serious enough, fail with an error.

### Environment and metadata

A snapflow `environment` tracks the pipe graph, and acts as a registry for the `modules`,
`runtimes`, and `storages` available to pipes. It is associated one-to-one with a single
`metadata database`. The primary responsibility of the metadata database is to track which
nodes have processed which DataBlocks, and the state of nodes. In this sense, the environment and
its associated metadata database contain all the "state" of a snapflow project. If you delete the
metadata database, you will have effectively "reset" your snapflow project. (You will
NOT have deleted any actual data produced by the pipeline, though it will be orphaned.)

### Component Development

Developing new snapflow components is straightforward and can be done as part of a snapflow
`module` or as a standalone component. Module development guide and tools coming soon.

### Type system details

Data blocks have three associated schemas:

- Inferred schema - the structure and datatypes automatically inferred from the actual data
- Nominal schema - the schema that was declared (or resolved, for a generic) in the pipe graph
- Realized schema - the schema that was ultimately used to physically store the data on a specific
  storage (the schema used to create a database table, for instance)

The realized schema is determined by the following factors:

- The setting of `CAST_TO_SCHEMA_LEVEL` to one of `hard`, `soft`, or `none`
- The discrepancies, if any, between the inferred schema and the nominal schema

The following table gives the logic for possible behavior of realized schema:

|                                                                                                         | none                                      | soft (default)                                                                                                                                            | hard                                                       |
| ------------------------------------------------------------------------------------------------------- | ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| inferred has subset of nominal fields                                                                   | realized is always equivalent to inferred | realized schema == nominal schema, but use inferred field definitions for extra fields.                                                                   | realized equivalent to nominal, extra fields are discarded |
| inferred is missing nullable fields from nominal                                                        | " "                                       | realized schema == nominal schema, but use inferred field definitions for extra fields, fill missing columns with NULL                                    | exception is raised                                        |
| inferred is missing non-nullable fields from nominal                                                    | " "                                       | exception is raised                                                                                                                                       | exception is raised                                        |
| inferred field has datatype mismatch with nominal field definition (eg string in a nominal float field) | " "                                       | realized schema is downcast to inferred datatype (and warning issued if `WARN_ON_DOWNCAST`). If `FAIL_ON_DOWNCAST` is set, an exception is raised instead | exception is raised                                        |
