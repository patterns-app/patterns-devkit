from __future__ import annotations

import base64
import dataclasses
import hashlib
import os
import re
import urllib.parse
from socketserver import BaseRequestHandler
from urllib.parse import ParseResult

import requests

from basis.cli.config import update_local_basis_config
from basis.cli.services.api import (
    AuthServer,
    get_auth_server,
)
from basis.cli.services.auth import (
    LOCAL_OAUTH_PORT,
    BaseOAuthRequestHandler,
    execute_oauth_flow,
)


def login():
    auth_server = get_auth_server()

    code_verifier = base64.urlsafe_b64encode(os.urandom(72)).decode("utf-8")
    code_verifier = re.sub("[^a-zA-Z0-9]+", "", code_verifier)

    state = base64.urlsafe_b64encode(os.urandom(72)).decode("utf-8")

    code_challenge = hashlib.sha256(code_verifier.encode("utf-8")).digest()
    code_challenge = base64.urlsafe_b64encode(code_challenge).decode("utf-8")
    code_challenge = code_challenge.replace("=", "")

    redirect_url = (
        f"http://localhost:{LOCAL_OAUTH_PORT}{LoginRequestHandler.handled_path}"
    )

    params = {
        "response_type": "code",
        "code_challenge_method": "S256",
        "code_challenge": code_challenge,
        "client_id": auth_server.devkit_client_id,
        "redirect_uri": redirect_url,
        "scope": "profile email openid offline_access",
        "audience": auth_server.audience,
        "state": state,
    }
    query = urllib.parse.urlencode(params)
    url = f"https://{auth_server.domain}/authorize?{query}"

    login_config = LoginConfig(
        auth_server=auth_server,
        state=state,
        code_verifier=code_verifier,
        redirect_url=redirect_url,
    )

    def on_request(handler: BaseRequestHandler):
        handler._login_config = login_config

    execute_oauth_flow(url, LoginRequestHandler, on_request)


@dataclasses.dataclass
class LoginConfig:
    auth_server: AuthServer
    state: str
    code_verifier: str
    redirect_url: str


class LoginRequestHandler(BaseOAuthRequestHandler):
    handled_path: str = "/auth_callback"
    _login_config: LoginConfig = None

    def handle_callback(self, parsed_url: ParseResult):
        qs = urllib.parse.parse_qs(parsed_url.query)
        if not (code := self.get_single_queryparam("code", qs)):
            return

        if not (state := self.get_single_queryparam("state", qs)):
            return

        login_config = self._login_config
        expected_state = login_config.state
        if state != expected_state:
            return self.finish_with_error(
                401,
                f"An invalid state was returned.  Expected {expected_state} but was {state}.  Unable to login",
            )

        # Exchange code for access & refresh tokens
        response = requests.post(
            f"https://{login_config.auth_server.domain}/oauth/token",
            json={
                "client_id": login_config.auth_server.devkit_client_id,
                "code_verifier": login_config.code_verifier,
                "code": code,
                "grant_type": "authorization_code",
                "redirect_uri": login_config.redirect_url,
            },
        )
        response.raise_for_status()

        json = response.json()
        if "refresh_token" not in json or "access_token" not in json:
            return self.finish_with_error(
                401, f"We did not receive a valid authorization result: {json}"
            )

        update_local_basis_config(
            auth_server=login_config.auth_server,
            refresh=json["refresh_token"],
            token=json["access_token"],
        )

        return self.finish_with_success(
            "Login successful", "Successfully logged in!  You may close this window."
        )
