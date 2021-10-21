import base64
import os
from pathlib import Path
from typing import Dict, List

from basis.cli.services.api import Endpoints, post, get


def list_objects(obj_type: str, organization_uid: str) -> List[Dict]:
    if obj_type == "env":
        endpoint = Endpoints.ENVIRONMENTS_LIST
    elif obj_type == "graph":
        endpoint = Endpoints.GRAPHS_LIST
    elif obj_type == "node":
        endpoint = Endpoints.NODES_LIST
    else:
        raise ValueError(obj_type)
    resp = get(endpoint, params={"organization_uid": organization_uid})
    resp.raise_for_status()
    return resp.json()
