from dataclasses import dataclass
from typing import Optional

DEFAULT_MODULE_KEY = "_local_"  # TODO: not ecstatic about how this works
VERSIONED_URIS = (
    False  # TODO: think about when we would want to use versioned URIs (never?)
)


@dataclass
class UriMixin:
    """
    URIs in Basis are of the form  [<component_type>:]<module_key>.<component_key>[@<version>]
    Examples:
        function:core.accumulate_as_dataset@1.0.3
        otype:common.Date
        payments.Transaction
        _local_.MyCustomType@0.1
    """

    key: str
    module_key: Optional[str]
    version: Optional[str]

    def __hash__(self):
        return hash(self.uri)

    @property
    def unversioned_uri(self):
        k = self.key
        module = self.module_key
        if not module:
            module = DEFAULT_MODULE_KEY
        k = module + "." + self.key
        return k

    @property
    def versioned_uri(self):
        uri = self.unversioned_uri
        if self.version:
            uri += "@" + str(self.version)
        return uri

    @property
    def uri(self):
        if VERSIONED_URIS:
            return self.versioned_uri
        return self.unversioned_uri


def is_uri(s: str) -> bool:
    return len(s.split(".")) == 2
