from __future__ import annotations

from typing import List

from requests import Session

from patterns.cli.services.api import Endpoints, post_for_json, get_json
from patterns.cli.services.pagination import paginated


def create_secret(
    organization_uid: str,
    name: str,
    value: str,
    description: str,
    sensitive: bool,
    session: Session = None,
) -> List[dict]:
    return post_for_json(
        Endpoints.org_secrets(organization_uid),
        json={
            "name": name,
            "value": value,
            "description": description,
            "sensitive": sensitive,
        },
        session=session,
    )


@paginated
def paginated_secrets(organization_uid: str, session: Session = None):
    return get_json(Endpoints.org_secrets(organization_uid), session=session)
