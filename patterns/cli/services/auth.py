from __future__ import annotations

import urllib.parse
import webbrowser
from abc import ABCMeta, abstractmethod
from datetime import timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import BaseRequestHandler
from threading import Barrier, Thread, BrokenBarrierError
from typing import Tuple, Callable, Optional, List, Dict, Type
from urllib.parse import ParseResult

from patterns.cli.services.output import abort, sprint

LOCAL_OAUTH_PORT = 30420

# Long-ish timeout.  If someone is signing up with an email / password, it can take a while.
REQUEST_TIMEOUT = timedelta(minutes=1)


def execute_oauth_flow(
    url: str,
    request_handler_class: Type[BaseOAuthRequestHandler],
    on_request: Callable[[BaseOAuthRequestHandler], None] | None = None,
):
    """Execute an oauth flow, opening a web browser and handling callbacks.

    Parameters:
        url: URL to open in the browser
        request_handler_class: Class which will handle the web request made by the
                               browser (for an oauth callback)
        on_request: Optional callback which is invoked before the actual handler is run
    """

    # noinspection PyTypeChecker
    server = OAuthHttpServer(
        ("localhost", LOCAL_OAUTH_PORT), request_handler_class, on_request
    )
    webbrowser.open(url, new=1, autoraise=True)

    server_thread = ServerThread(server)
    server_thread.start()

    try:
        server.wait_for_request_started()
    except BrokenBarrierError:
        server.shutdown()
        abort("Timed out waiting for the web browser to hit callback url")

    try:
        server.wait_for_request_finished()
    except BrokenBarrierError:
        server.shutdown()
        abort("Timed out waiting for the request to finish")

    server.shutdown()
    server_thread.join(timeout=5)

    if server.error_result:
        abort(server.error_result)
    elif server.success_result:
        sprint(f"[info]{server.success_result}")
    else:
        abort("OAuth server finished without an error or success result")


class ServerThread(Thread):
    def __init__(self, server: OAuthHttpServer):
        super().__init__()
        self._server = server

    def run(self):
        self._server.serve_forever()


class OAuthHttpServer(HTTPServer):
    """Utility base class for coordinating between handlers and the CLI"""

    def __init__(
        self,
        server_address: Tuple[str, int],
        request_handler_class: Callable[..., BaseRequestHandler],
        on_request_cb: Callable[[BaseRequestHandler], None] | None,
    ):
        super().__init__(server_address, request_handler_class)
        self._on_request_cb = on_request_cb
        self.error_result = None
        self.success_result = None

        timeout_seconds = REQUEST_TIMEOUT.total_seconds()
        self._request_started_barrier = Barrier(2, timeout=timeout_seconds)
        self._request_finished_barrier = Barrier(2, timeout=timeout_seconds)

    def wait_for_request_started(self):
        self._request_started_barrier.wait()

    def wait_for_request_finished(self):
        self._request_finished_barrier.wait()

    def on_request(self, handler: BaseRequestHandler):
        if self._on_request_cb:
            self._on_request_cb(handler)

    def finish_with_error(self, error_result: str):
        self.error_result = error_result

    def finish_with_success(self, success_result: str):
        self.success_result = success_result


class BaseOAuthRequestHandler(BaseHTTPRequestHandler, metaclass=ABCMeta):
    @property
    @abstractmethod
    def handled_path(self) -> str:
        """The path which this handler will process ('/some_callback')"""
        ...

    @abstractmethod
    def handle_callback(self, parsed_url: ParseResult):
        ...

    @property
    def oauth_http_server(self) -> OAuthHttpServer:
        # noinspection PyTypeChecker
        return self.server

    def do_GET(self):
        parsed_url = urllib.parse.urlparse(self.path)

        if parsed_url.path == self.handled_path:
            self.oauth_http_server.on_request(self)
            self.oauth_http_server._request_started_barrier.wait()
            try:
                self.handle_callback(parsed_url)
            finally:
                self.oauth_http_server._request_finished_barrier.wait()
        else:
            # We don't finish_with_error here, because the browser might request
            # all kinds of things (CORS OPTIONS or favicon.ico, etc.) in addition
            # to the request we really want.  If there is a real problem here,
            # the managing thread will shut down the server
            self.send_html_response(404, f"Unhandled path: {parsed_url.path}")

    def get_single_queryparam(
        self, param_name: str, params: Dict[str, List[str]]
    ) -> Optional[str]:
        if (
            not param_name in params
            or len(params[param_name]) != 1
            or not isinstance(params[param_name][0], str)
        ):
            self.finish_with_error(
                500, f"Invalid {param_name} in response: {params.get(param_name)}"
            )
            return None
        return params[param_name][0]

    def send_html_response(self, code: int, html: str):
        self.send_response(code)
        self.send_header("Content-type", "text/html")
        self.end_headers()

        self.wfile.write(bytes(f"<html><body>{html}</body></html>", "utf-8"))

    def finish_with_error(self, status_code: int, error_result: str):
        self.send_html_response(
            status_code, "An error occurred.  See the console logs for details."
        )
        self.oauth_http_server.finish_with_error(error_result)

    def finish_with_success(self, success_result: str, success_browser_html: str):
        self.send_html_response(200, success_browser_html)
        self.oauth_http_server.finish_with_success(success_result)
