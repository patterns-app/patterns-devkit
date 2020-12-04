
![snapflow](https://github.com/kvh/snapflow/workflows/snapflow/badge.svg)

<p>&nbsp;</p>
<p align="center">
  <img width="400" src="assets/snapflow.svg">
</p>
<p>&nbsp;</p>

### Modern Data Pipelines

Snapflow is a framework for building **end-to-end functional data pipelines from modular
components**. Snapflow abstracts over underlying database, runtime, and storage resources with
functional, type-aware data graphs that operate on streams of **immutable** `datablocks`. These graphs are
composed of discrete `pipes` written in python or SQL that operate on `datablocks` and connect to
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

 - **Reusable modules and components** - There are hundreds of `pipes` and `schemas` ready to
   plug into pipelines in the snapflow repository [Coming soon].
   
    - Connect Stripe data to LTV models and plot a cohort chart
    - Blend stock prices and with macroeconomic data from FRED
    - Export SaaS metrics to Google Sheets, Tableau, or Looker
    
   and much more, instantly and out of the box.
     
 - **Testable components** - Modular `pipes` allow individual steps in a data process to be
   independently tested and QA'd with the same rigor as software. 
     
 - **Flexibility** - snapflow lets you build data flows on and across any database or file system.
   It works with big or small data, in both batch and streaming modes.
     
 - **Zero cost abstractions and high performance** - snapflow makes its type and immutability
  guarantees at the abstraction level, so those guarantees can be compiled away at execution time
  for high performance. This lets developers and analysts work with clean mental models without
  incurring performance costs at runtime. The snapflow compiler also optimizes across databases,
  runtimes, and storages -- e.g. a query on BigQuery vs Redshift, data copy on S3 vs in-memory
  -- and can optimize entire pipelines for the resources at hand, leading to overall performance
  gains when adopting snapflow.
  
N.B.: snapflow is **ALPHA** software. Expect breaking changes to core APIs. 


### Example

`pip install snapflow snapflow-stripe snapflow-bi`

```python
from snapflow import Environment, node
from snapflow_stripe import stripe
from snapflow_bi import bi


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
```

### Architecture overview

Key elements of snapflow:

#### Datablocks

A `datablock` is an immutable set of data records of uniform `schema` -- think csv file
or database table. `datablocks` are the basic data unit of snapflow, the unit that `pipes` take
as input and produce as output. Once created a datablock's data will never change: no records will
be added or deleted or data points modified. More precisely, `datablocks` are a reference to an
abstract ideal of the records; in practice, a DataBlock is stored on one or more `storage`
mediums in one or more `dataformats` -- a CSV on the local file, a JSON string in memory, or a
table in a Postgres database, for example. Snapflow abstracts over specific formats and storage
engines, and provides conversion and i/o between them while maintaining the integrity of the data.

#### Pipes

`pipes` are the core computational unit of snapflow. They are functions that operate on
`datablocks` and are added as nodes to a pipe graph, linking one node's output to another's
input via `streams`. Pipes are written in python or sql. Below are two equivalent and valid pipes:
 
```python
def sales(txs: DataBlock) -> DataBlock:  
    df = txs.as_dataframe()
    return df.groupby("customer_id").sum("amount")
```

```sql
select
    customer_id
  , sum(amount)
from txs
group by customer_id
```

#### Schemas

`schemas` define data schemas (similar to a database table schema) that let `pipes` specify the data
 structure they expect and allow them to inter-operate safely. They also
provide a natural place for field descriptions, validation logic, deduplication
behavior, and other metadata associated with a specific type of data record.

`Schemas` behave like _interfaces_ in other languages. The snapflow type system is structurally and
gradually typed -- types are both optional and inferred, there is no formal type hierarchy, and
type compatibility can be inspected at runtime. A type is `compatible` with another
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

We could also specify `relationships` and `implementations` of this type to other types:

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
timeseries data, say a seasonal adjustment pipe, can now be applied to this `Order` data. We could
also apply this schema implementations at declaration time with the `schema_translation` arg:
 
 ```python
orders = node("orders", order_source)
n = node(
    "seasonal_adj",
    seasonal_adjustment,
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
def sales(txs: DataBlock[Transaction]) -> DataBlock[CustomerMetric]:  
    df = txs.as_dataframe()
    return df.groupby("customer_id").sum("amount")
```

In SQL, we add type hints with comments after the `select` statement and after table identifiers:

```sql
-- In SQL, we use comments to specify expected schemas
select --:CustomerMetric
    customer_id
  , sum(amount)
from txs -- :Transaction
group by customer_id
```

Typing is always optional, our original pipe definitions were valid with
no annotated `schemas`. Snapflow `schemas` are a powerful mechanism for producing reusable
components and building maintainable large-scale data projects and ecosystems. They are always
optional though, and should be used when the value they provide out-weighs the friction they
introduce.


#### Streams

Datablock `streams` connect nodes in the pipe graph. By default each node's output is a simple
stream of datablocks. These streams can be modified though with stream `operators`:

```python
from snapflow import node
from snapflow.operators import merge, filter

n1 = node("n1", source1)
n2 = node("n2", source2)
combined = merge(n1, n2)
big_blocks = filter(combined, function=lambda block: block.count() > 1000)
n3 = node("n3", do_something, upstream=big_blocks)
```

Common operators include `latest`, `merge`, `filter`. It is simple to create your own. 


## Other concepts

### Consistency and Immutability
To the extent possible, snapflow maintains the same data and byte representation of `datablock`
records across formats and storages. Not all formats and storages support all data representations,
though -- for instance, empty / null / None / NA support differs
significantly across common data formats, runtimes, and storage engines. When it notices a
conversion or storage operation may produce data loss or corruption, snapflow will try to emit a
warning or, if serious enough, fail with an error. 
 
### Environment and metadata
A snapflow environment tracks the pipe graph, and acts as a registry for the `modules`,
`runtimes`, and `storages` available to pipes. It is associated one-to-one with a single
`metadata database`.  The primary responsibility of the metadata database is to track which
pipes have processed which DataBlocks, and the state of pipes. In this sense, the environment and
its associated metadata database contain all the "state" of a snapflow project. If you delete the
 metadata database, you will have effectively "reset" your snapflow project.

### Component Development

Developing new snapflow components is straightforward and can be done as part of a snapflow `module` or as
a standalone component. 


### Type system

Data blocks have three associated schemas:
 - inferred schema - the structure and datatypes automatically inferred from the actual data
 . inference is hard and prone to error and discrepancies
 - nominal schema - the schema that was declared (or resolved for generics) in the pipe graph
 - realized schema - the schema that was ultimately used to physically store the data on a specific
   storage (the schema used to create a database table, for instance)
  
The realized schema is determined by the following factors:
 - The setting of CAST_TO_SCHEMA to one of "hard", "soft", or "none"
 - The discrepancies, if any, between the inferred schema and the nominal schema
 

The following table gives the logic for possible behavior of realized schema:

|                                                                                                           | none                                       | soft (default)                                                                                                                                               | hard                                                                   |
|-----------------------------------------------------------------------------------------------------------|--------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------|
| inferred has subset of nominal fields                                                                     | realized is always equivalent to inferred  | realized schema will be the nominal schema, using inferred field definitions for extra fields.                                                               | realized always equivalent to nominal. extra fields will be discarded  |
| inferred is missing nullable fields from nominal                                                          | realized is always equivalent to inferred  | realized schema will be the nominal schema, using inferred field definitions for extra fields, filling missing columns with NULL                             | exception will be raised (OR same as soft?)                            |
| inferred is missing non-nullable fields from nominal                                                      | realized is always equivalent to inferred  | exception will be raised                                                                                                                                     | exception will be raised                                               |
| inferred has datatype inconsistency with nominal field definition (eg a string in a nominal float field)  | realized is always equivalent to inferred  | realized schema will downcast to inferred datatype (and issue warning if WARN_ON_DOWNCAST). If FAIL_ON_DOWNCAST is set, an exception will instead be raised  | exception will be raised                                               |