from __future__ import annotations

import json
import os
from datetime import date, datetime
from typing import Any, Callable, Dict, Generic, Iterator, List, Optional, Type, Union

import requests
from loguru import logger
from ratelimit import limits, sleep_and_retry
from requests import Response


"""
DEPRECATED - See helpers/extraction
"""


class JsonHttpApiConnection:
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


class SimpleTestJsonHttpApiConnection:
    responses: List[str]
    empty_response: Dict = {}
    expected_high_water_mark: datetime
    expected_record_count: int

    def __init__(self):
        self.response_count = 0
        self.json_responses: List[Dict] = []
        for response in self.responses:
            if isinstance(response, Dict):
                json_resp = response
            elif isinstance(response, str):
                file_path = os.path.join(os.path.dirname(__file__), response)
                with open(file_path) as f:
                    json_resp = json.load(f)
            else:
                raise Exception(f"Response unsupported {response}")
            self.json_responses.append(json_resp)

    def get(self, url: str, params: Dict) -> Union[Dict, List]:
        if self.response_count >= len(self.json_responses):
            return self.empty_response
        resp = self.json_responses[self.response_count]
        self.response_count += 1
        return resp
