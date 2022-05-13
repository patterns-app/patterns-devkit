from __future__ import annotations

import urllib.parse
from urllib.parse import ParseResult

from patterns.cli.config import read_devkit_config, update_devkit_config
from patterns.cli.services.auth import (
    LOCAL_OAUTH_PORT,
    BaseOAuthRequestHandler,
    execute_oauth_flow,
)


def logout():
    cfg = read_devkit_config()
    if not cfg.auth_server:
        return

    params = {
        "client_id": cfg.auth_server.devkit_client_id,
        "returnTo": f"http://localhost:{LOCAL_OAUTH_PORT}{LogoutRequestHandler.handled_path}",
    }

    query = urllib.parse.urlencode(params)
    url = f"https://{cfg.auth_server.domain}/v2/logout?{query}"

    execute_oauth_flow(url, LogoutRequestHandler)


class LogoutRequestHandler(BaseOAuthRequestHandler):
    handled_path: str = "/logout_callback"

    def handle_callback(self, parsed_url: ParseResult):
        update_devkit_config(refresh=None, token=None, auth_server=None)
        self.finish_with_success(
            "Successfully logged out", "You have successfully logged out"
        )
