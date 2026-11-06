"""Example: dynamic fan-out. Check a dozen product prices in parallel.

``price`` is mapped over the ``skus`` list: dagflow runs it once per SKU, at most
``-j`` at a time, retries each SKU on its own, and hands ``cheapest`` the list of
results. Run it with:

    dagflow run examples/price_check.py -j 4
    dagflow ui examples/price_check.py
"""

from __future__ import annotations

import asyncio
from typing import Any

from dagflow import Context, Skip, Workflow

workflow = Workflow("price_check")

_CATALOG = {
    "SKU-01": ("Desk lamp", 39.0),
    "SKU-02": ("Office chair", 189.0),
    "SKU-03": ("Monitor arm", 74.5),
    "SKU-04": ("USB-C hub", 32.0),
    "SKU-05": ("Webcam", 58.0),
    "SKU-06": ("Mechanical keyboard", 119.0),
    "SKU-07": ("Noise-cancelling headphones", 249.0),
    "SKU-08": ("Laptop stand", 45.0),
    "SKU-09": ("Fax machine", 0.0),  # discontinued
    "SKU-10": ("Desk mat", 21.0),
    "SKU-11": ("Cable tray", 18.5),
    "SKU-12": ("Footrest", 29.0),
}
_FLAKY_SKU = "SKU-06"
_DISCONTINUED_SKU = "SKU-09"


@workflow.task(tags={"extract"})
async def skus(category: str = "office") -> list[str]:
    """Pretend to list the SKUs in a category."""
    await asyncio.sleep(0.1)
    return list(_CATALOG)


@workflow.task(map="skus", retries=2, backoff=0.1, retry_on=(ConnectionError,), tags={"extract"})
async def price(skus: str, ctx: Context, discount: float = 0.0) -> dict[str, Any]:
    """Fetch one SKU's price. Runs once per SKU; one SKU fails on its first try."""
    name, base = _CATALOG[skus]
    # Different latencies per item make the parallel timeline easy to see.
    await asyncio.sleep(0.1 + (ctx.index or 0) % 4 * 0.06)
    if skus == _FLAKY_SKU and ctx.attempt == 1:
        raise ConnectionError(f"pricing API timed out for {skus}")
    if skus == _DISCONTINUED_SKU:
        raise Skip(f"{skus} is discontinued")
    return {"sku": skus, "name": name, "price": round(base * (1 - discount), 2)}


@workflow.task(tags={"transform"})
def cheapest(price: list[dict[str, Any]], top_n: int = 3) -> list[dict[str, Any]]:
    return sorted(price, key=lambda item: item["price"])[:top_n]


@workflow.task(tags={"load"})
def report(cheapest: list[dict[str, Any]], price: list[dict[str, Any]]) -> str:
    rows = "\n".join(f"  {p['sku']}  {p['name']:<14} ${p['price']:>7.2f}" for p in cheapest)
    text = f"Checked {len(price)} products. Cheapest:\n{rows}"
    print(text)
    return text
