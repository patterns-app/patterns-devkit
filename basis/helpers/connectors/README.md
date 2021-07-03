WIP - Helpers for building source connection functions

# API fetching basics

To fetch an API and keep all records up-to-date we need:

- reliable way to track incremental progress and restart where last left off
- OR efficient enough / small enough to refetch in entirety every time

Second case is usually trivial to implement -- no state is tracked -- but is
very rarely a viable option, given indeterminate nature of data set size. With
some APIs we have no choice but to use this second option, they do not meet the criteria below for tracking
incremental progress, so we have to hope we can redownload efficiently enough, or
we are forced to sacrifice with potentially broken / missing data.

Crux for first case is "reliable way to track incremental progress". For this to be
guaranteed to work we need at a minimum either:

1.  Immutable records and sortable, filterable, strictly monotonic stable attribute or set of attributes
2.  OR mutable records and sortable, filterable updated timestamp with other unique sortable attribute (strictly monotonic)

Let's go through the three constaints on our selected attribute(s):

- Sortable: we must be able to sort by the selected attributes.
- Filterable: we must be able to specify records beyond where we left off last time
- Strictly monotonic: selected attributes should be strictly monotonic in combination, meaning no duplicate values.
  To see why, imagine using created_at alone, and one million records get created in a
  back fill at once with same created_at. Now the fetcher must consume 1 million records without
  stopping or error, or it will have to perpetually start over and refetch.

Unfortunately many (most?) APIs do not fully support these requirements for reliably tracking incremental
progress. There are several strategies we can take to mitigate the harm from this, mostly by relaxing our
requirements as little as necessary:
If mutable records AND:

- No way to sort and/or filter by updated_at (this is bad API design and the provider should be notified and encouraged to fix!)
- Refetching all records occasionally to get updates, defining a certain duration of acceptable staleness
- If sortable by created_at: Refetching all records for a certain amount of time after creation
- No strict monotonic set of sortable, filterable attributes
- This constraint can be relaxed probabilistically -- if no more than ~typical size of fetch~ duplicates
  are expected, then should not be a problem. Hard to guarantee this to be the case in general for a given API, but often no
  way around it. This failure will manifest as stunted fetch progress and may be hard to diagnose.

### Race conditions

There are futher complications with mutable records: these records may change in the middle of
a fetching operation. This is especially likely if doing a long-running series of pagination
based fetches sorted by updated_at: a record may be updated in the middle of this operation,
shifting what set of records belong to a "page", thus potentially missing records.
For this reason, we should never paginate mutable records. For the rarer case of a record updating
mid api call, we are in a tough spot: only a forced refetching of old data will catch this miss.

# General recommendations:

Given the discussion above, we can conclude that 1) most APIs do not allow for guaranteed
incremental fetching, 2) refetching entire datasets everytime is prohibitively expensive for
real-world dataset sizes. This leaves us in a tough place of making tradeoffs. Our goal
should be to embrace these tradeoffs and give the end user visibility, understanding, and control over
these tradeoffs.

### Solution 1: For immutable records

- Sort by strictly monotonic field if avialable (autoincrment ID, for instance), otherwise by created timestamp
  - track and store latest field value fetched, refetching that value _inclusive_ next time if not strictly monotonic
- If no sort available, refetch everything every time

### Solution 2: For mutable records

- Sort by combination of strictly monotonic field and updated timestamp
  if avialable (autoincrment ID and updated_at, for instance). It is rare for an API to support this operation.
  - If we have strictly monotonic attributes, then we do not want to follow pagination. Simply refetch each time with new
    filters
- Otherwise, sort by monotonic update timestamp if available
- In either case, track and store latest field values fetched, refetching at those values _inclusive_ next time if not strictly monotonic
- If no sort available, refetch everything every time
- In any case, mutable records mean that any given API call may miss concurrent record updates. This means for guaranteed accuracy,
  we must in all cases periodically refetch all data. This "Double check for record updates every X days", or
  "maximum staleness" can be configured by the end user, along with a "until X days old" setting (if filterable create timestamp)

### Solution 3: Treat mutable records as immutable records

- it may be the case that the end user does not care, or cares little, about record updates
- it is possible to provide both an immutable and mutable interface to the same API endpoint
- in this case, we'll again ask the end user to choose a "Check records for updates every" setting

### Solution 4: General solution for all sources

- Create multiple importers for same source of differing frequency and re-fetch window
- For example, one importer checks for new records in last day every 5 minutes,
  another re-imports the last week every hour, and a final one imports all-time once a day
- Pros:
  - works for any source that can sort and filter by a monotonic field, regardless of mutability
  - fairly obvious and configurable to end user
  - has reasonable tradeoff of freshness vs effort
- Cons:
  - No real guarantee on freshness / accuracy other than after total re-fetch
  - If source has frequently updating records that are very old (no "curing window"), data will be out-of-date more often

# Import tooling

## Http connection

- url:
  - base url + endpoint
  - get_url
- params:
  - default params
  - date format
  - default headers
  - get_params
  - remove empty params
  - remove None params
- rate limiting:
  - limit calls per minute
  - backoff timeout seconds
  - ratelimit params
- handling:
  - raise for status
  - retryable exceptions
  - response_has_retryable_error
  - response_has_error

## Records generation and response handling

Scenarios:

- paginated records response
  - page based
  - limit offset based
  - cursor based
- sub-paginated records response
  - parent call required to make sub-calls
- per-record request and response
- bulk download (csv, gz, etc)

- get records obj from req response
- get next request from req response and records
- records exhausted from req response records

## Refresh behavior

Modes:

- Get all every time (no state)
  - least efficient but sometimes only option
- Get unseen updated records
  - state: latest modification date imported
  - required: filter and sort by modification field
- Get unseen created records (won't get updates if records have them)
  - state: latest creation date imported
  - required: filter and sort by creation field
  - optional: "curing window"

Refresh variables:

- created_at_field
- created_at_is_filterable_and_sortable
- modified_at_field
- modified_at_is_filterable_and_sortable
- curing window
- minimum refresh time (source won't update more frequently than this?)
- update params with filter and sort
- get latest created at from records ()
- set latest created at
- get latest modified at from records ()
- set latest modified at
- get state
- update state

StripeApiConnection:
baseurl: ...
...

StripeChargesConnection:
baseurl:...
endpoint: charges

StripePaginatedJsonHandler(PaginatedJsonHandler)
pagination method = "page"

    def get records obj ()

    get next request():
        has more

StripeImportAll(ImportAll)
...

StripeImportNewlyCreated(ImportNewlyCreated)
created_at_field = created

    def add filters and sorts (params, latest created at)
        pass

    def get latest created at from records
