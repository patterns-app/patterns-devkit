
![dags](https://github.com/kvh/basis/workflows/dags/badge.svg)

<img src="assets/dags-logo-simple.svg" widht="100%">

### Modern Data Pipelines
 
Dags is a framework for building end-to-end functional data pipelines from modular components.
Dags abstracts over underlying database, runtime, and storage resources with **functional,
type-aware data graphs**. These graphs are composed of discrete `pipes` written in
**python or SQL** that snap together both batch and streaming data flows to form end-to-end data
 pipelines, from API extraction to SQL transformation to analysis, modeling, and visualization.

Dags `pipes` optionally expose type interfaces, called `ObjectSchemas` (or just `schemas`), that
describe the structure, data types, and semantics of the expected input and output data. These type
interfaces allow the Dags community to build an ecosystem of interoperable components that power
instant "plug and play" pipelines.
 
Dags brings the best practices learned over the last 60 years in software to the world of data,
with the goal of global collaboration, reproducible byte-perfect results, and performance at any
scale from laptop to AWS cluster.

### Features:

 - **Reusable modules and components** - There are hundreds of `pipes` and `schemas` ready to
  plug into pipelines in the Dags Repository [Coming soon].
   
    - Connect Stripe data to LTV models and plot a chart
    - Blend finance and macroeconomics data
    - Export SaaS metrics to Google Sheets, Tableau, or Looker
    
   and much more, instantly and out of the box.
     
 - **Testable components** - Modular `pipes` allow individual steps in a data process to be
  independently tested and QA'd with the same rigor as software. 
     
 - **Batch or incremental** - Dags exposes both batch and streaming data outputs from all pipes
 , allowing chains of incremental and batch work to exist naturally with strong guarantees on
  accuracy and completeness.
     
 - **Zero cost abstractions and high performance** - Dags makes its type and immutability
  guarantees at the abstraction level, so those guarantees can be compiled away at execution time
  for high performance. This lets developers and analysts work with clean mental models without
  incurring performance costs at runtime. The Dags compiler also optimizes across databases,
  runtimes, and storages -- e.g. a query on BigQuery vs Redshift, data copy on S3 vs in-memory
  -- and can optimize entire pipelines for the resources at hand, leading to overall performance
  gains when adopting Dags.
  
Dags is **ALPHA** software. Expect breaking changes to core APIs. 


### Example

`pip install dags dags-stripe dags-bi`

```python
from dags import Environment, Graph
from dags_stripe import stripe
from dags_bi import bi


env = Environment("sqlite:///dags.db")
graph = Graph(env)
graph.add_node(
    key="stripe_txs",
    pipe=stripe.extract_charges,
    config={"api_key":"xxxxxxxx"},
)
graph.add_node(
    key="ltv_model",
    pipe=bi.transaction_ltv_model,
    upstream="stripe_txs",
)
ltv_output = env.produce(graph)
print(ltv_output.as_dataframe())
```

### Architecture overview

Key elements of Dags:

#### pipe

`pipes` are the core computational unit of Dags. They are added as nodes to a pipe
 graph, linking one node's output to another's input. Pipes are written in python or sql and
 can be as simple or complex as required. Below are two equivalent and valid pipes:
 
```python
def sales(txs: DataSet):  
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

Pipes can operate on *incremental* or *batch* inputs, and generate incremental or batch output.
In Dags, incremental data is processed as *DataBlock*s and batch data is processed as
 *DataSet*s. These are discussed in more detail below.

#### ObjectSchema

`ObjectSchemas` define data schemas that let `pipes` specify the data structure
 they expect and allow them to inter-operate safely. They also
provide a natural place for column descriptions, validation logic, deduplication
 behavior, and other metadata associated with
a specific type of data record. You can think of `ObjectSchemas` as the equivalent of "Interfaces"
in a traditional programming language paradigm -- they specify a "contract" that the underlying data
must abide by. The Dags `ObjectSchema` system is "duck" typed and "gradually" typed -- types are both
optional and inferred, there is no formal type hierarchy, and type compatibility can be inspected
at runtime. A type is said to be `compatible` with another type if it defines a
superset of compatible fields or if it provides an `implementation` of that type.

A minimal type example, in yaml:

```yaml
name: Transaction
version: 1.0
unique on: id
on duplicate: ReplaceWithNewer
fields:
  id:
    type: Unicode(256)
    validators:
      - NotNull
  amount:
    type: Numeric(12,2)
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

`pipes` can then declare the ObjectSchemas they expect, allowing them to specify the
 (minimal) contract of their interfaces. Type annotating our earlier examples would look like this:
 
```python
# In python, use type annotations to specify expected ObjectSchema:  
def sales(txs: DataBlock[Transaction]) -> DataSet[CustomerMetric]:  
    df = txs.as_dataframe()
    return df.groupby("customer_id").sum("amount")
```

In SQL, we add type hints with comments after the `select` statement and after table identifiers:

```sql
-- In SQL, use special syntax to specify expected ObjectSchema
select --: DataSet[CustomerMetric]
    customer_id
  , sum(amount)
from txs -- :DataBlock[Transaction]
group by customer_id
```

A few things to note:
    - Typing is always optional, our original pipe definitions were valid with no ObjectSchemas
    - Whether or not explicit schemas are provided, Dags always infers the actual
      structure of the data automatically, producing an `AutoSchema` that can then be validated
      against an explicitly provided schema or used directly instead.
    - We've taken some liberties with Python's type hints (partly why Dags requires python 3.7+) -- 
      we don't require our ObjectSchema types to exist as actual python type objects in scope.
 
Dags `ObjectSchema`s are a powerful mechanism for producing reusable components and building
maintainable large-scale data projects and ecosystems. They are always optional though, and
should be used when the value they provide out-weighs the friction they introduce.


#### DataBlock

A `DataBlock` is an immutable set of data records of a uniform `ObjectSchema`. `DataBlocks ` are the
basic data unit of Dags, the unit that `pipes` take as input and ultimately produce as
output. More precisely, `DataBlocks` are a reference to an abstract ideal of these records; in
practice, a DataBlock will be stored on one or more Storage mediums in one or more DataFormats -- a
CSV on the local file, a JSON string in memory, or a table in a Postgres database, for example --
Dags abstracts over specific formats and storage engines, and provides seamless
conversion and i/o between them.

To the extent possible, Dags maintains the same data and byte representation of these records
across formats and storages. Not all formats and storages support all data representations,
though -- for instance, empty / null / None / NA support differs
significantly across common data formats, runtimes, and storage engines. When it notices a
conversion or storage operation may produce data loss or corruption, Dags will try to emit a
warning or, if serious enough, fail with an error. 
 
 
#### DataSet

`DataBlock`s are the basic data unit of Dags -- their discrete, immutable qualities make them
ideal for building industrial grade pipelines and complex ecosystems of `pipes`. But
often it is necessary or simpler to work not with incremental chunks of data, but with entire
datasets as a whole. Dags `DataSets` serve this purpose -- they "accumulate"
DataBlocks of a uniform ObjectSchema, deduping and merging records according to specified or default
logic, and provide a clean, named set of data records (a single `customers` table, for instance).
 
 
### Incremental vs Batch (DataBlocks vs DataSets)

Every `pipe` in Dags automatically outputs both an incremental `DataBlock` stream and a
batch accumulated `DataSet`. Downstream pipes can connect to either the DataBlock stream or
the DataSet by specify in their python or sql type signature which mode of input and output
they expect.

For example:
 
```python
# Incrementally process each `emails` DataBlock as it comes, joining with the customers `DataSet`
def add_name(emails: DataBlock, customers: DataSet) -> DataBlock:
    emails_df = emails.as_dataframe()
    customers_df = customers.as_dataframe()
    emails_df["name"] = emails_df.join(customers_df, on="customer_id")["name"]
    return emails_df

# Or batch process every time
def add_name_batch(emails: DataSet, customers: DataSet) -> DataSet:
    emails_df = emails.as_dataframe()
    customers_df = customers.as_dataframe()
    emails_df["name"] = emails_df.join(customers_df, on="customer_id")["name"]
    return emails_df
```

Note that since we want to join emails on all possible customers, we used a `DataSet` input in both
cases and only processed the emails incrementally in the first scenario.


#### Environment and metadata
A Dags environment tracks the pipe graph, and acts as a registry for the `modules`,
`runtimes`, and `storages` available to pipes. It is associated one-to-one with a single
`metadata database`.  The primary responsibility of the metadata database is to track which
pipes have processed which DataBlocks, and the state of pipes. In this sense, the environment and
its associated metadata database contain all the "state" of a Dags project. If you delete the
 metadata database, you will have effectively "reset" your Dags project.


### Component Development

Developing new Dags components is straightforward and can be done as part of a Dags `module` or as
a standalone component. 
