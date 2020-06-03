from dataclasses import dataclass
from typing import Optional

DEFAULT_MODULE_KEY = "_local_"  # TODO: not ecstatic about how this works


@dataclass(frozen=True)
class UriMixin:
    key: str
    module_key: Optional[str]
    version: Optional[str]

    def __hash__(self):
        return hash(self.versioned_uri)

    @property
    def uri(self):
        k = self.key
        module = self.module_key
        if not module:
            module = DEFAULT_MODULE_KEY
        k = module + "." + self.key
        return k

    @property
    def versioned_uri(self):
        uri = self.uri
        if self.version:
            uri += "@" + str(self.version)
        return uri


def is_uri(s: str) -> bool:
    return len(s.split(".")) == 2


"function:shopify.transform_order_items@1.0.3"
"otype:shopify.ShopifyOrderJson@0.9.4"


# def uridataclass(fn):
#     fn = dataclass(frozen=True)(fn)
#     fn.__hash__ =
