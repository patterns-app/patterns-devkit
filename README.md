<h1 style="font-size: 80px">BASIS</h1>

### "The World's First Operating System for Data"
 
BASIS is a framework for building end-to-end data ETL pipelines from modular components. BASIS
abstracts over underlying database, runtime, and storage resources with **functional,
type-aware data graphs**. These graphs are composed of discrete `DataFunctions` written in python or
SQL operating on streams of immutable `DataBlocks` -- sets of data records of uniform `ObjectType`.

The power of BASIS lies in its Component ecosystem powered by its flexible type system, which
provides universal data interfaces, called **ObjectTypes** or _**otypes**_, that allow
interoperability and modularity of data operations. BASIS brings the best practices learned over
the last 60 years in software to the world of data.

Global collaboration, reproducible byte-perfect results, and performance at any
scale from laptop to AWS cluster -- this is **BASIS**.

### Features:

 - **Reusable modules and components**  
   There are hundreds of `DataFunctions`, `Sources`, and `Targets` ready to snap together in
   the BASIS Repository [Coming soon].
   
    - Connect Stripe data to LTV models
    - Blend finance and macroeconomics data
    - Export SaaS metrics to Google Sheets or Looker
    
   and much more, instantly and out of the box. BASIS supports the entire POP data pipeline (or
   "POPline"): **P**ull from external sources, **O**perate on the data, and **P**ush to end
   user applications.
  
 - **Stateless data pipelines**  
   `DataFunctions` operate statelessly on immutable `DataBlocks` for guaranteed reproducibility
   and correctness in ETLs. Developing powerful new `DataFunctions` is simple with isolated code
   and well-defined interfaces.  
     
 - **Testable components**  
   Modular `DataFunctions` allow individual steps in a data process to be independently tested and
   QA'd with the same rigor as software. All components available in the BASIS Repository are
   automatically tested against sample data sets of the appropriate `ObjectType`.
     
 - **Global interoperability**  
   Reuse best-in-class ETLs, models, and analysis built by developers and analysts from around
   the world. No need to reinvent the wheel.
     
 - **Zero cost abstractions and high performance**  
   BASIS makes its type and immutability guarantees at the abstraction level, so those
   guarantees can be compiled away at execution time for high performance. This lets developers and
   analysts work with clean mental models without incurring performance costs at runtime. The
   BASIS compiler also allows for modeling relative runtime and storage operation costs -- e.g. a
   query on BigQuery vs Redshift, data copy on S3 vs in-memory -- and can optimize entire pipelines
   for the resources at hand, leading to overall performance gains when adopting BASIS.
  
BASIS is 0.1.0 **alpha** software, and has only existed a few months. Expect breaking changes to
core APIs. 
