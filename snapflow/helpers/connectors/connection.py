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
from backoff import expo, on_exception
from loguru import logger
from ratelimit import RateLimitException, limits, sleep_and_retry
from requests import Response
from requests.exceptions import RequestException
from requests.models import Request
from snapflow.utils.typing import T, V


"""

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
        backoff_timeout_seconds: int = 90,
        remove_none_params: bool = True,
        remove_empty_params: bool = True,
        ratelimit_params: Dict = None,
    ):
        self.default_params = default_params or {}
        self.default_headers = default_headers or {}
        self.date_format = date_format
        self.raise_for_status = raise_for_status
        self.ratelimit_calls_per_min = ratelimit_calls_per_min
        self.ratelimit_params = ratelimit_params
        self.backoff_timeout_seconds = backoff_timeout_seconds
        self.get = self.add_rate_limiting(self.get)
        self.remove_none_params = remove_none_params
        self.remove_empty_params = remove_empty_params

    def add_rate_limiting(self, f: Callable):
        if self.ratelimit_params:
            g = limits(**self.ratelimit_params)(f)
        else:
            g = limits(calls=self.ratelimit_calls_per_min, period=60)(f)
        g = sleep_and_retry(g)
        g = on_exception(
            expo,
            (RateLimitException, RequestException),
            max_time=self.backoff_timeout_seconds,
            factor=4,
        )(g)
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
            if self.remove_empty_params and isinstance(v, str) and not v.strip():
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
