from __future__ import annotations

from patterns.cli.services.api import Endpoints, get_json
from patterns.cli.services.pagination import paginated


@paginated
def paginated_graphs(organization_uid: str):
    return get_json(Endpoints.graphs_list(organization_uid))
