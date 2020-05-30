# Basis

### The Open-source Operating System for Data
 
Basis is a framework for building end-to-end data pipelines from modular components. Basis
abstracts over underlying database, runtime, and storage resources with **functional, type
-aware data graphs**. These graphs are composed of `DataFunctions` written in python or SQL
operating on streams of immutable `DataBlocks` -- sets of data records of uniform `ObjectType`.

The power of Basis lies in its flexible type system, which provides universal data type interfaces, 
called **ObjectTypes** or _**otypes**_, that allow interoperability and modularity of data
operations. Basis brings the best practices learned over the last 60 years in software to the
world of data.

Global collaboration, reproducible byte-perfect results, and performance at any
scale from laptop to AWS cluster -- this is **Basis**.

### Features:

 - **Reusable modules and components**  
   There are hundreds of `DataFunctions`, `Sources`, and `Targets` ready to snap together in
   the Basis Repository [Coming soon]. Connect Stripe data to LTV models, blend finance data and
   macroeconomics, export SaaS metrics to Google Sheets or Looker, and much more, instantly and
   out of the box. Basis supports the entire POP data flow ("POPline" as we call it): **P**ull
   from external sources, **O**perate on the data, and **P**ush to end user applications.
  
 - **Stateless data pipelines**  
   `DataFunctions` operate statelessly on immutable `DataBlocks` for guaranteed reproducibility
   and correctness in ETLs. Developing powerful new `DataFunctions` is simple with isolated code
   and well-defined interfaces.  
     
 - **Testable components**  
   Modular `DataFunctions` allow individual steps in a data process to be independently tested and
   QA'd with the same rigor as software. All components available in the Basis Repository are
   automatically tested against sample data sets of the appropriate `ObjectType`.
     
 - **Global interoperability**  
   The Basis `otype` system allows reuse of best-in-class ETLs, models, and analysis built by
   developers and analysts from around the world. No need to reinvent the wheel.
     
 - **Zero cost abstractions and high performance**  
   Basis makes its type and immutability guarantees at the abstraction level, so those
   guarantees can be compiled away at execution time for high performance. This lets developers and
   analysts work with clean mental models without incurring performance costs at runtime. The
   Basis compiler also allows for modeling relative runtime and storage operation costs -- e.g. a
   query on BigQuery vs Redshift, data copy on S3 vs in-memory -- and can optimize entire pipelines
   for the resources at hand, leading to overall performance gains when adopting Basis.
  
Basis is 0.1.0 **alpha** software, and has only existed a few months. Expect breaking changes to
core APIs. 
