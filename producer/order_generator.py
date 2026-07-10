import random
import time
import uuid

_PRODUCTS = [
    ("SKU-TSHIRT", 24.99),
    ("SKU-HOODIE", 59.99),
    ("SKU-MUG", 14.99),
    ("SKU-CAP", 19.99),
    ("SKU-TOTE", 12.99),
    ("SKU-STICKER-PACK", 6.99),
    ("SKU-SOCKS", 9.99),
    ("SKU-POSTER", 17.99),
]

# Fixed pool of customer ids so repeated orders from "the same customer"
# are possible, this is what makes the unique-customers-per-window metric
# meaningful instead of every order being a new customer.
_CUSTOMER_POOL = [f"cust-{i:05d}" for i in range(2000)]


def generate_order() -> dict:
    num_items = random.randint(1, 4)
    line_items = []
    for _ in range(num_items):
        sku, unit_price = random.choice(_PRODUCTS)
        qty = random.randint(1, 3)
        line_items.append({"sku": sku, "qty": qty, "unit_price": unit_price})

    total = round(sum(li["qty"] * li["unit_price"] for li in line_items), 2)

    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": random.choice(_CUSTOMER_POOL),
        "line_items": line_items,
        "total": total,
        "timestamp": time.time(),
    }
