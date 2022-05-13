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
from requests import HTTPError

from patterns.cli.config import update_devkit_config
from patterns.cli.services.api import (
    AuthServer,
    get_auth_server,
)
from patterns.cli.services.auth import (
    LOCAL_OAUTH_PORT,
    BaseOAuthRequestHandler,
    execute_oauth_flow,
)


def login():
    auth_server = get_auth_server()

    # The code_verifier and code_challenge are part of the OAuth PKCE spec.
    # https://datatracker.ietf.org/doc/html/rfc7636#section-4.1
    # https://developer.okta.com/blog/2019/08/22/okta-authjs-pkce
    #
    # We make some random bits (code_verifier), hash it (code_challenge) and send
    # the challenge with our initial authorize request.  When the user is redirected
    # back, we post the unhashed value (code_verifier) when obtaining the
    # token.  The server then hashes the code_verifier to match the initial value that
    # was sent by the redirected client.
    #
    # This flow stands in the place of having a client secret, since the devkit is
    # Open Source we cannot use client secret based auth.
    #
    # The encoded verifier only needs 32 bytes of entropy, but we use 33 because
    # otherwise we end up with padding on our bas64 encoded value
    code_verifier = base64.urlsafe_b64encode(os.urandom(33)).decode("utf-8")

    code_challenge = hashlib.sha256(code_verifier.encode("utf-8")).digest()
    code_challenge = base64.urlsafe_b64encode(code_challenge).decode("utf-8")
    # Remove padding (per Auth0 samples)
    code_challenge = code_challenge.replace("=", "")

    # Random state is sent with the initial request and received on the redirect
    # and is supposed to mitigate CSRF attacks:
    # https://auth0.com/docs/secure/attack-protection/state-parameters
    state = os.urandom(50).hex()

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

        update_devkit_config(
            auth_server=login_config.auth_server,
            refresh=json["refresh_token"],
            token=json["access_token"],
        )

        return self.finish_with_success(
            "Login successful", "Successfully logged in!  You may close this window."
        )
