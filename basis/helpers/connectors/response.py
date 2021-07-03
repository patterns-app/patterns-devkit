class PaginatedResponse:
    pass


class PaginatedResponse:
    get_records_obj


class ImporterContext:
    function_context: DataFunctionContext
    latest_response: HttpResponse
    latest_request: HttpRequest


class StripeImport:
    get_next_request: CursorPaginator
    emit_state: CursorPaginator


from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, TYPE_CHECKING, Iterator

from dcp.data_format import Records
from dcp.utils.common import ensure_datetime, utcnow
from requests.auth import HTTPBasicAuth
from basis import datafunction, Context, DataBlock
from basis.core.extraction.connection import JsonHttpApiConnection

if TYPE_CHECKING:
    from basis_stripe import StripeChargeRaw


STRIPE_API_BASE_URL = "https://api.stripe.com/v1/"
MIN_DATE = datetime(2006, 1, 1)


@dataclass
class ImportStripeChargesState:
    latest_full_import_at: datetime
    current_starting_after: str


def stripe_importer(
    endpoint: str,
    ctx: Context,
    api_key: str,
    curing_window_days: int = None,
    extra_params: Dict = None,
):
    """
    Stripe only allows fetching records in one order: from newest to oldest,
    so we use its cursor based pagination to iterate once through all historical.

    Stripe doesn't have a way to request by "updated at", so we must
    refresh old records according to our own logic, using a "curing window"
    to re-import records up to one year (the default) old.
    """
    latest_full_import_at = ctx.get_state_value("latest_full_import_at")
    latest_full_import_at = ensure_datetime(latest_full_import_at)
    current_starting_after = ctx.get_state_value("current_starting_after")
    params = {
        "limit": 100,
    }
    if extra_params:
        params.update(extra_params)
    # if earliest_created_at_imported <= latest_full_import_at - timedelta(days=int(curing_window_days)):
    if latest_full_import_at and curing_window_days:
        # Import only more recent than latest imported at date, offset by a curing window
        # (default 90 days) to capture updates to objects (refunds, etc)
        params["created[gte]"] = int(
            (
                latest_full_import_at - timedelta(days=int(curing_window_days))
            ).timestamp()
        )
    if current_starting_after:
        params["starting_after"] = current_starting_after
    conn = JsonHttpApiConnection()
    endpoint_url = STRIPE_API_BASE_URL + endpoint
    all_done = False
    while ctx.should_continue():
        resp = conn.get(endpoint_url, params, auth=HTTPBasicAuth(api_key, ""))
        json_resp = resp.json()
        assert isinstance(json_resp, dict)
        records = json_resp["data"]
        if len(records) == 0:
            # All done
            all_done = True
            break

        # Return acutal data
        yield records

        latest_object_id = records[-1]["id"]
        if not json_resp.get("has_more"):
            all_done = True
            break
        params["starting_after"] = latest_object_id
        ctx.emit_state_value("current_starting_after", latest_object_id)
    else:
        # Don't update any state, we just timed out
        return
    # We only update state if we have fetched EVERYTHING available as of now
    if all_done:
        ctx.emit_state_value("latest_imported_at", utcnow())
        # IMPORTANT: we reset the starting after cursor so we start from the beginning again on next run
        ctx.emit_state_value("current_starting_after", None)


@datafunction(
    "import_charges", namespace="stripe", display_name="Import Stripe charges",
)
def import_charges(
    ctx: Context, api_key: str, curing_window_days: int = 90,
) -> Iterator[Records[StripeChargeRaw]]:
    yield from stripe_importer("charges", ctx, api_key, curing_window_days)
