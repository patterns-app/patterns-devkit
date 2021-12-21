from typing import Callable, Iterable, List

from basis.cli.services.api import get_json


def paginated(fn: Callable[..., dict]) -> Callable[..., Iterable[List[dict]]]:
    """Decorator that yields pages from a paginated endpoint"""

    def f(*args, **kwargs) -> Iterable[List[dict]]:
        data = fn(*args, **kwargs)
        yield data["results"]
        while data["next"]:
            data = get_json(data["next"], base_url="")
            if data["results"]:
                yield data["results"]

    return f
