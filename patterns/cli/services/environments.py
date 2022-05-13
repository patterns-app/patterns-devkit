from __future__ import annotations

from patterns.cli.services.api import Endpoints, get_json
from patterns.cli.services.pagination import paginated


def get_environment_by_name(organization_uid: str, name: str) -> dict:
    return get_json(Endpoints.environment_by_slug(organization_uid, name))


def get_environment_by_id(environment_uid: str) -> dict:
    return get_json(Endpoints.environment_by_id(environment_uid))


@paginated
def paginated_environments(environment_uid: str):
    return get_json(Endpoints.environments_list(environment_uid))
