import functools
import itertools
from typing import Callable, Iterable, List

from patterns.cli.services.api import get_json


class PaginatedCall:
    def __init__(self, initial_request: Callable[[], dict]):
        self._initial_request = initial_request

    def pages(self) -> Iterable[List[dict]]:
        """Iterate over pages returned from the endpoint"""
        data = self._initial_request()
        yield data["results"]
        while data["next"]:
            data = get_json(data["next"], base_url="")
            if data["results"]:
                yield data["results"]

    def __iter__(self) -> Iterable[dict]:
        """Iterate over all objects returned from the endpoint"""
        return itertools.chain.from_iterable(self.pages())


def paginated(fn: Callable[..., dict]) -> Callable[..., PaginatedCall]:
    """Decorator that yields pages from a paginated endpoint"""

    def f(*args, **kwargs):
        return PaginatedCall(functools.partial(fn, *args, **kwargs))

    return f
