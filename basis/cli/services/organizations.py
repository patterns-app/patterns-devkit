from __future__ import annotations

from basis.cli.services.api import Endpoints, get_json
from basis.cli.services.pagination import paginated


def get_organization_by_name(name: str) -> dict:
    return get_json(Endpoints.organization_by_name(name))


def get_organization_by_id(organization_uid: str) -> dict:
    return get_json(Endpoints.organization_by_id(organization_uid))


@paginated
def paginated_organizations():
    return get_json(Endpoints.ORGANIZATIONS_LIST)
