from __future__ import annotations

from requests import Session

from patterns.cli.services.api import Endpoints, get_json
from patterns.cli.services.pagination import paginated


def get_organization_by_name(name: str, session: Session = None) -> dict:
    return get_json(Endpoints.organization_by_slug(name), session=session)


def get_organization_by_id(organization_uid: str, session: Session = None) -> dict:
    return get_json(Endpoints.organization_by_id(organization_uid), session=session)


@paginated
def paginated_organizations(session: Session = None):
    return get_json(Endpoints.ORGANIZATIONS_LIST, session=session)
