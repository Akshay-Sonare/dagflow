"""Example: build a sales report from three (simulated) data sources.

Run it with:

    dagflow run examples/sales_report.py -p region=EU -p top_n=3
    dagflow graph examples/sales_report.py
    dagflow ui examples/sales_report.py
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections import defaultdict
from typing import Any

from dagflow import Workflow

workflow = Workflow("sales_report")

# Per thread, because every dashboard run gets its own thread: concurrent runs
# then can't use up each other's successful calls.
_order_api = threading.local()


@workflow.task(tags={"extract"})
async def customers(region: str = "EU") -> list[dict[str, Any]]:
    """Pretend to page through a CRM API."""
    await asyncio.sleep(0.3)
    names = ["Ada", "Grace", "Linus", "Guido", "Barbara", "Ken"]
    return [{"id": i, "name": name, "region": region} for i, name in enumerate(names, 1)]


@workflow.task(retries=3, backoff=0.1, retry_on=(ConnectionError,), tags={"extract"})
async def orders() -> list[dict[str, Any]]:
    """A flaky upstream: two of every three calls fail, the retry policy rides it out."""
    await asyncio.sleep(0.2)
    # Modulo, not "first two calls", so the demo stays flaky on every run of a
    # long-lived process such as `dagflow ui`.
    _order_api.calls = getattr(_order_api, "calls", 0) + 1
    if _order_api.calls % 3:
        raise ConnectionError("orders API returned 503")
    return [
        {"customer_id": cid, "amount": amount, "currency": cur}
        for cid, amount, cur in [
            (1, 120.0, "EUR"), (2, 80.0, "USD"), (1, 45.5, "GBP"), (3, 300.0, "EUR"),
            (4, 99.9, "USD"), (5, 15.0, "EUR"), (2, 250.0, "GBP"), (6, 60.0, "USD"),
        ]
    ]  # fmt: skip


@workflow.task(timeout=2, tags={"extract"})
def rates() -> dict[str, float]:
    """Blocking I/O is fine: sync tasks run on a worker thread."""
    time.sleep(0.25)
    return {"EUR": 1.0, "USD": 0.92, "GBP": 1.17}


@workflow.task(cache=True, tags={"transform"})
def orders_eur(orders: list[dict[str, Any]], rates: dict[str, float]) -> list[dict[str, Any]]:
    return [{**o, "amount": round(o["amount"] * rates[o["currency"]], 2)} for o in orders]


@workflow.task(tags={"transform"})
def revenue_by_customer(
    customers: list[dict[str, Any]], orders_eur: list[dict[str, Any]]
) -> dict[str, float]:
    names = {c["id"]: c["name"] for c in customers}
    totals: dict[str, float] = defaultdict(float)
    for order in orders_eur:
        totals[names[order["customer_id"]]] += order["amount"]
    return dict(totals)


@workflow.task(tags={"transform"})
def top_customers(revenue_by_customer: dict[str, float], top_n: int = 3) -> list[tuple[str, float]]:
    ranked = sorted(revenue_by_customer.items(), key=lambda kv: kv[1], reverse=True)
    return [(name, round(total, 2)) for name, total in ranked[:top_n]]


@workflow.task(tags={"load"})
def report(top_customers: list[tuple[str, float]], region: str = "EU") -> str:
    rows = "\n".join(
        f"  {i}. {name:<8} €{total:>8.2f}" for i, (name, total) in enumerate(top_customers, 1)
    )
    text = f"Top customers ({region})\n{rows}"
    print(text)
    return text
