import base64
import os
from pathlib import Path
from typing import Dict, List

from basis.cli.services.api import Endpoints, get, post


def list_objects(obj_type: str, organization_name: str) -> List[Dict]:
    if obj_type == "env":
        obj_type = "environment"
    endpoint = getattr(Endpoints, f"{obj_type.upper()}S_LIST")
    resp = get(endpoint, params={"organization_name": organization_name})
    resp.raise_for_status()
    return resp.json()
