<img src="basis.png">

### Modern Data Pipelines
 
BASIS is a framework for building end-to-end functional data pipelines from modular components.
BASIS abstracts over underlying database, runtime, and storage resources with **functional,
type-aware data graphs**. These graphs are composed of discrete `DataFunctions` written in
**python or SQL** that snap together both batch and streaming data flows to form end-to-end data
 pipelines, from API data extraction to SQL transformation to machine learning models.

BASIS `DataFunctions` optionally expose a type interface, called an `ObjectType`, that describes the
 structure and semantics of the expected input and output data. These type interfaces
 allow the BASIS community to build an ecosystem of components that anyone can reuse project to
  project, across organizations and industries.
 
BASIS brings the best practices learned over the last 60 years in software to the world of data,
with the goal of global collaboration, reproducible byte-perfect results, and performance at any
scale from laptop to AWS cluster.

### Features:

 - **Reusable modules and components**  
   There are hundreds of `DataFunctions`, types, and external data `Sources` and `Targets` ready to
    snap together in the BASIS Repository [Coming soon].
   
    - Connect Stripe data to LTV models
    - Blend finance and macroeconomics data
    - Export SaaS metrics to Google Sheets, Tableau, or Looker
    
   and much more, instantly and out of the box.
     
 - **Testable components**  
   Modular `DataFunctions` allow individual steps in a data process to be independently tested and
   QA'd with the same rigor as software. All components available in the BASIS Repository are
   automatically tested against sample data sets of the appropriate `ObjectType`.
     
 - **Batch or streaming**
   Basis exposes both batch and streaming data outputs from all DataFunctions, allowing chains of
   incremental and batch work to exist naturally with strong guarantees on accuracy and
    completeness.
   
 - **Global interoperability**  
   Reuse best-in-class ETLs, models, and analysis built by developers and analysts from around
   the world. No need to reinvent the wheel.
     
 - **Zero cost abstractions and high performance**  
   BASIS makes its type and immutability guarantees at the abstraction level, so those
   guarantees can be compiled away at execution time for high performance. This lets developers and
   analysts work with clean mental models without incurring performance costs at runtime. The
   BASIS compiler also optimizes across databases, runtimes, and storages -- e.g. a
   query on BigQuery vs Redshift, data copy on S3 vs in-memory -- and can optimize entire pipelines
   for the resources at hand, leading to overall performance gains when adopting BASIS.
  
NB: BASIS is 0.1.1 **ALPHA** software. Expect breaking changes to core APIs. 


### Getting started

`pip install git+git://github.com/kvh/basis.git`

To initialize a Basis project in a directory:

`basis init`

Edit the resulting `project.py` file to add storages, runtimes, and modules.

Build your pipeline (you'll need to add the `stripe` and `bi` modules to your project):

```python
from basis import current_env
from basis_modules import stripe, bi

graph = Graph(current_env)
graph.add_external_source_node(
    name="stripe_txs",
    external_source=stripe.StripeTransactionsResource,
    config={"api_key":"xxxxxxxx"},
)
graph.add_node(
    name="ltv_model",
    function=bi.TransactionLTVModel,
    inputs="stripe_txs",
)
```

Then run it:

`basis run`


### Architecture overview

The key elements of Basis:

#### DataFunction

`DataFunction`s are the core computational unit of Basis. They are added as nodes to a function
 graph and then linked by connecting their inputs and outputs. They are written in python or sql and
 can be arbitrarily simple or complex. Below are two equivalent and valid (though untyped) DataFunctions:
 
```python
def sales(txs: DataBlock) -> DataFrame:  
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

Functions can operate on *incremental* or *batch* inputs, and generate incremental or batch output.
In Basis, incremental data is processed as *DataBlock*s and batch data is processed as
 *DataSet*s. These are discussed in more detail below.

#### ObjectType

`ObjectType`s define data schemas that let `DataFunction`s specify the data structure
 they expect and allow them to inter-operate safely. They also
provide a natural place for column descriptions, validation logic, deduplication
 behavior, and other metadata associated with
a specific type of data record. You can think of `ObjectType`s as the equivalent of "Interfaces"
in a traditional programming language paradigm -- they specify a "contract" that the underlying data
must abide by. The Basis `ObjectType` system is "duck" typed and "gradually" typed -- types are both
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

`DataFunction`s can then declare the ObjectTypes they expect, allowing them to specify the
 (minimal) contract of their interfaces. Type annotating our earlier examples would look like this:
 
```python
# In python, use type annotations to specify expected ObjectType:  
def sales(txs: DataBlock[Transaction]) -> DataFrame[CustomerMetric]:  
    df = txs.as_dataframe()
    return df.groupby("customer_id").sum("amount")
```

```sql
-- In SQL, use special syntax to specify expected ObjectType
select:CustomerMetric
    customer_id
  , sum(amount)
from txs:Transaction
group by customer_id
```

A few things to note:
    - typing is always optional, our original function definitions were valid with no ObjectTypes
    - We've taken some liberties with Python's type hints (hence why Basis requires python 3.7+)
    - We've introduced a special syntax for typing SQL queries: `table:Type` for inputs and
     `select:Type` for output.
 
Basis `ObjectType`s are a powerful mechanism for producing reusable components and building
maintainable large-scale data projects and ecosystems. They are always optional though, and
should be used when the value they provide out-weighs the friction they introduce.


#### DataBlock

A `DataBlock` is an immutable set of data records of uniform `ObjectType`. `DataBlock`s are the
basic data unit of Basis, the unit that `DataFunction`s take as input and ultimately produce as
output. More precisely, `DataBlock`s are a reference to an abstract ideal of these records. In
 practice, a DataBlock will be stored on one or more Storage mediums in one or more DataFormats -- a
  CSV on the local file, a JSON string in memory, or a
table in a Postgres database, for example. To the extent possible, Basis maintains the 
same data and byte representation of these records across formats and storages. For some formats and
data types this is simply not possible, and Basis tries to emit warnings in these cases. 
 
 
#### DataSet

`DataBlock`s are the basic data unit of Basis -- their discrete, immutable properties make them
ideal for building industrial grade pipelines and complex ecosystems of `DataFunction`s. But
 often you it is necessarily or just simpler to work with entire datasets in batch, not as
  incremental chunks. Basis `DataSet`s serve this purpose -- they "accumulate"
DataBlocks of a uniform ObjectType, deduping and merging records according to desired logic, and
 provide a clean, nicely named, end set of data records (a single `customers` table, for instance).
 
Every `DataFunction` in Basis automatically outputs both an incremental `DataBlock` stream and a
batch accumulated `DataSet`. Downstream functions can connect to either the DataBlock stream or
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


#### ExternalResource

Basis handles all stages of the ETL pipeline, including extracting data from external sources. An
`ExternalResource` is any source of data external to the Basis pipeline. This could be a SaaS API
 (Stripe, Facebook Ads, Zendesk, Shopify, etc), an external production database, a CSV, or a
  Google Sheet, for example. `ExternalResource`s are configured and added to an environment as
   follows:
  
```python
env.add_external_source_node(
    name="stripe_txs",
    external_source="stripe.StripeTransactionsResource",
    config={"api_key": "xxxxxxxx"},
)
```

This is a shortcut for the more explicit:

```python
cfgd_provider = stripe.external.StripeProvider(api_key="xxxxxxx")
cfgd_resource = cfgd_provider.StripTransactionsResource()
env.add_node(
    name="stripe_txs",
    function=cfgd_resource.extractor,
)
```

`ExternalResource`s are **stateful** entities in Basis -- Basis must keep track of what it has
extracted from the `ExternalResource` so far, and how it will extract more in the future. Every
`ExternalResource` has an associated default `Extractor` (which is just a specific type of
`DataFunction`) that updates and utilizes this `ExternalResource` state to fetch and stay in-sync
with the external data. Working with external systems is a complex and subtle topic (we often have 
limited visibility into the state and changes of the external system). Read the docs on
`ExternalResource`s and `Extractor`s for more details [Coming soon].

#### Environment
A Basis environment tracks the DataFunction graph, and acts as a registry for the `modules`,
`runtimes`, and `storages` available to DataFunctions. It is associated one-to-one with a single
`metadata database`.  The primary responsibility of the metadata database is to track which
DataFunctions have processed which DataBlocks, and the state of ExternalResources. In this
sense, the environment and its associated metadata database contain all the "state" of a Basis
project. If you delete the metadata database, you will have effectively "reset" your Basis
project.


### Component Development

Developing new Basis components is straightforward and can be done as part of a Basis `module` or as
a standalone component. We'll start with a simple standalone example.

Say we want to use the DataFunction we developed in an earlier example in a data pipeline:

```python
def sales(txs: DataBlock[Transaction]) -> DataFrame[CustomerMetric]:  
    df = txs.as_dataframe()
    return df.groupby("customer_id").sum("amount")
```

