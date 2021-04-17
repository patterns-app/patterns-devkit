from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Type,
    Union,
)

import requests
from loguru import logger
from ratelimit import limits, sleep_and_retry
from requests import Response
from requests.models import Request
from snapflow.utils.typing import T, V


"""
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
 1. Immutable records and sortable, filterable, strictly monotonic stable attribute or set of attributes
 2. OR mutable records and sortable, filterable updated timestamp with other unique sortable attribute (strictly monotonic)

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

Race conditions
There are futher complications with mutable records: these records may change in the middle of
a fetching operation. This is especially likely if doing a long-running series of pagination
based fetches sorted by updated_at: a record may be updated in the middle of this operation,
shifting what set of records belong to a "page", thus potentially missing records.
For this reason, we should never paginate mutable records. For the rarer case of a record updating
mid api call, we are in a tough spot: only a forced refetching of old data will catch this miss.

General recommendations:

Given the discussion above, we can conclude that 1) most APIs do not allow for guaranteed
incremental fetching, 2) refetching entire datasets everytime is prohibitively expensive for
real-world dataset sizes. This leaves us in a tough place of making tradeoffs. Our goal
should be to embrace these tradeoffs and give the end user visibility, understanding, and control over
these tradeoffs.

Solution 1: For immutable records
 - Sort by strictly monotonic field if avialable (autoincrment ID, for instance), otherwise by created timestamp
   - track and store latest field value fetched, refetching that value *inclusive* next time if not strictly monotonic
 - If no sort available, refetch everything every time

Solution 2: For mutable records
 - Sort by combination of strictly monotonic field and updated timestamp
   if avialable (autoincrment ID and updated_at, for instance). It is rare for an API to support this operation.
    - If we have strictly monotonic attributes, then we do not want to follow pagination. Simply refetch each time with new
      filters
 - Otherwise, sort by monotonic update timestamp if available
 - In either case, track and store latest field values fetched, refetching at those values *inclusive* next time if not strictly monotonic
 - If no sort available, refetch everything every time
 - In any case, mutable records mean that any given API call may miss concurrent record updates. This means for guaranteed accuracy,
   we must in all cases periodically refetch all data. This "Double check for record updates every X days", or
   "maximum staleness" can be configured by the end user, along with a "until X days old" setting (if filterable create timestamp)

Solution 3: Treat mutable records as immutable records
 - it may be the case that the end user does not care, or cares little, about record updates
 - it is possible to provide both an immutable and mutable interface to the same API endpoint
 - in this case, we'll again ask the end user to choose a "Check records for updates every" setting

Solution 4: General solution for all sources
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
"""


class RecordsExhausted(Exception):
    pass


@dataclass
class FetchStrategy:
    incremental: bool
    sort_fields: List[str] = None
    ignore_records_after_x_hours_old: int = None


class HttpApiConnection:
    def __init__(
        self,
        default_params: Dict = None,
        default_headers: Dict = None,
        date_format: str = "%F %T",
        raise_for_status: bool = True,
        ratelimit_calls_per_min: int = 1000,
        remove_none_params: bool = True,
    ):
        self.default_params = default_params or {}
        self.default_headers = default_headers or {}
        self.date_format = date_format
        self.raise_for_status = raise_for_status
        self.ratelimit_calls_per_min = ratelimit_calls_per_min
        self.g = self.add_rate_limiting(self.get)
        self.remove_none_params = remove_none_params

    def add_rate_limiting(self, f: Callable):
        g = sleep_and_retry(f)
        g = limits(calls=self.ratelimit_calls_per_min, period=60)(g)
        return g

    def get_default_params(self) -> Dict:
        return self.default_params.copy()

    def get_default_headers(self) -> Dict:
        return self.default_headers.copy()

    def validate_params(self, params: Dict) -> Dict:
        formatted = {}
        for k, v in params.items():
            if self.remove_none_params and v is None:
                continue
            if isinstance(v, datetime) or isinstance(v, date):
                v = v.strftime(self.date_format)
            formatted[k] = v
        return formatted

    def get(
        self, url: str, params: Dict = None, headers: Dict = None, **kwargs
    ) -> Response:
        default_params = self.get_default_params()
        if params:
            default_params.update(params)
        default_headers = self.get_default_headers()
        if headers:
            default_headers.update(headers)
        final_params = self.validate_params(default_params)
        resp = requests.get(url, params=final_params, headers=headers, **kwargs)
        if self.raise_for_status:
            resp.raise_for_status()
        return resp


@dataclass
class FetchProgress:
    key: str = None
    latest_created_at_field_fetched: str = None
    latest_modified_at_field_fetched: str = None
    latest_ordered_field_fetched: str = None
    extra: Dict[str, str] = None


class ApiConnection(HttpApiConnection):
    records_are_immutable: bool = False
    created_at_field: str = None
    created_at_is_filterable_and_sortable_asc: bool = False
    modified_at_field: str = None
    modified_at_is_filterable_and_sortable_desc: bool = False
    double_check_records_for_updates_every_x_hours: int = 48
    double_check_records_for_updates_until_x_hours_old: int = 24 * 7
    # Probably don't include these in documented API, too confusing and so rarely usable
    unique_field: str = None
    unique_is_filterable_and_sortable: bool = False
    supports_multi_field_sort: bool = (
        False  # Can sort WITH modified_at in same call. Rarely the case
    )
    pagination_method: str = "page"  # or "cursor" or "none"
    default_query_params: Dict[str, Any] = None
    default_headers: Dict[str, Any] = None
    datetime_format: str = "%F %T"
    query_params_dataclass: Type = None  # ???
    progress_dataclass: Type = FetchProgress
    ratelimit_calls_per_min: int = 1000
    raise_error_for_status: bool = True

    def get_data_object_from_response(self, resp: Response) -> V:
        "Return list of dicts, file buffer, or other supported DataFormat"
        raise NotImplementedError

    def update_request_for_next_call(self, req: Request, resp: Response):
        "Increase page number, set next cursor or url, etc"
        raise NotImplementedError

    def update_progress(
        self, prev_progress: FetchProgress, req: Request, resp: Response, data_obj: V
    ) -> FetchProgress:
        "Returns an updated progress state based on request and response"
        raise NotImplementedError

    def prepare_request(
        self, url: str, query_params: Dict[str, Any], headers: Dict[str, Any]
    ) -> Any:
        headers = self.prepare_headers(headers)
        query_params = self.prepare_query_params(query_params)
        req = Request(url=url, params=query_params, headers=headers)
        return req

    def add_filters_and_sorts(self, params: Dict) -> Dict:
        return params

    def prepare_query_params(self, params: Dict) -> Dict:
        final_params = self.get_default_headers()
        final_params.update(params)
        params = self.add_filters_and_sorts(params)
        final_params = self.clean_params(final_params)
        return final_params

    def clean_params(self, params: Dict) -> Dict:
        formatted = {}
        for k, v in params.items():
            if self.remove_none_params and v is None:
                continue
            if isinstance(v, datetime) or isinstance(v, date):
                v = v.strftime(self.date_format)
            formatted[k] = v
        return formatted

    def run(self, progress: FetchProgress) -> Iterable[V]:
        raise NotImplementedError


class FetchIncrementalImmutableRecords(HttpApiConnection):
    """
    Use this connector for endpoints that return immutable records (eg events)
    and have a created_at field ("created at" or incrementing id) that is
    filterable and sortable ascending.
    """

    created_at_field: str = None
    created_at_is_filterable_and_sortable_asc: bool = (
        False  # Must be True, otherwise use RefetchAllNew
    )
    default_query_params: Dict[str, Any] = None

    def add_filters_and_sorts(self, params: Dict, progress: FetchProgress) -> Dict:
        if not self.created_at_field:
            raise NotImplementedError(
                "Cannot fetch incremental immutable without created_at field"
            )
        if not self.created_at_is_filterable_and_sortable_asc:
            raise NotImplementedError(
                "Cannot fetch incremental immutable without sortable + filterable created_at field"
            )
        params[self.created_at_field]
        return params

    def run(self, progress: FetchProgress) -> Iterable[V]:
        pass


class FetchIncrementalMutableRecords(HttpApiConnection):
    """
    Use this connector for endpoints that return mutable records (eg fulfillments)
    and have a "modified at" field that is filterable and sortable descending.

    Because "modified at" fields do not always track all updates (eg for denormalized
    objects), and because calls may miss concurrent updates, it is recommended to
    refetch records to catch missed updates for some period of time after record creation.
    Use the `refetch_records_until_x_hours_old` to configure this.
    """

    modified_at_field: str = None
    modified_at_is_filterable_and_sortable_desc: bool = (
        False  # Must be True, otherwise use RefetchAllNew
    )
    refetch_records_until_x_hours_old: int = 24 * 7  # For missed updates
    default_query_params: Dict[str, Any] = None


class RefetchAllNew(HttpApiConnection):
    """
    Use this connector for endpoints that do not support the required
    filtering and sorting options for the incremental fetchers, but DO
    support a *filterable* creation-ordered field.

    This works like RefetchAll but will not refetch old rows that
    are not expected to update again. For immutable records, use
    refetch_records_until_x_hours_old to provide a buffer from
    when a "created at" is set and when the record actually shows
    up in the api (usually only a few seconds hopefully, but can be days).
    For mutable records, this fetcher will only catch updates made
    within the `refetch_records_until_x_hours_old` window. If you want
    to catch all updates on all mutable records, you must use RefetchAll.
    """

    created_at_field: str = None
    created_at_is_filterable: bool = False  # Must be True
    refetch_records_until_x_hours_old: int = None
    default_query_params: Dict[str, Any] = None


class RefetchAll(HttpApiConnection):
    """
    Use this connector for endpoints that do not support any
    required filtering or sorting options. It will attempt to refetch
    all records everytime run.
    """

    default_query_params: Dict[str, Any] = None
