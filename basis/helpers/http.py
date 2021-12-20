from __future__ import annotations

from datetime import date, datetime
from typing import Callable

import requests

try:
    from ratelimit import limits, sleep_and_retry
except ImportError:
    limits = None
    sleep_and_retry = None
from requests import Response


class JsonHttpApiConnection:
    def __init__(
        self,
        default_params: dict = None,
        default_headers: dict = None,
        date_format: str = "%F %T",
        raise_for_status: bool = True,
        ratelimit_calls_per_min: int = 1000,
        remove_none_params: bool = True,
        method: str = "get",
    ):
        self.default_params = default_params or {}
        self.default_headers = default_headers or {}
        self.date_format = date_format
        self.raise_for_status = raise_for_status
        self.ratelimit_calls_per_min = ratelimit_calls_per_min
        self.g = self.add_rate_limiting(self.get)
        self.remove_none_params = remove_none_params
        self.method = method

    def add_rate_limiting(self, f: Callable):
        if limits is None:
            return f
        g = sleep_and_retry(f)
        g = limits(calls=self.ratelimit_calls_per_min, period=60)(g)
        return g

    def get_default_params(self) -> dict:
        return self.default_params.copy()

    def get_default_headers(self) -> dict:
        return self.default_headers.copy()

    def validate_params(self, params: dict) -> dict:
        formatted = {}
        for k, v in params.items():
            if self.remove_none_params and v is None:
                continue
            if isinstance(v, datetime) or isinstance(v, date):
                v = v.strftime(self.date_format)
            formatted[k] = v
        return formatted

    def get(
        self, url: str, params: dict = None, headers: dict = None, method=None, **kwargs
    ) -> Response:
        method = method or self.method
        default_params = self.get_default_params()
        if params:
            default_params.update(params)
        final_headers = self.get_default_headers()
        if headers:
            final_headers.update(headers)
        final_params = self.validate_params(default_params)
        if method == "get":
            resp = requests.get(
                url, params=final_params, headers=final_headers, **kwargs
            )
        elif method == "post":
            final_headers["Content-Type"] = final_headers.get(
                "Content-Type", "application/json"
            )
            resp = requests.post(
                url, json=final_params, headers=final_headers, **kwargs
            )
        else:
            raise NotImplementedError(self.method)
        if self.raise_for_status:
            resp.raise_for_status()
        return resp

    def post(self, *args, **kwargs):
        return self.get(*args, **kwargs, method="post")
