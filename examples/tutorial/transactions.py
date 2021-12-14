from datetime import datetime
from basis import node, OutputStream, State
import random


@node
def generate_transactions(
        transactions_raw=OutputStream,
        state=State
):
    year = state.get_state_value("year") or 2000
    for i in range(100):
        record = {
            "id": i,
            "amount": random.random() * 100,
            "customer_id": i % 10,
            "processed_at": datetime(year, i % 11 + 1, i % 26 + 1),
        }
        transactions_raw.append_record(record)
    state.set_state_value("year", year + 1)
