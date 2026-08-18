"""Level 1 — Synthetic raw-data generator for the Greenplum warehouse.

The DuckDB version generated data *inside the engine* (range(), random(),
normal draws). Greenplum has no such vectorized generator, so this module
synthesizes the same dataset in Python — same schema, same distributions,
same injected data-quality problems — and streams it to data/raw/ as
CSV + JSONL, ready for the standard Greenplum bulk-load path (COPY FROM /
external tables).

The generator deliberately injects realistic data-quality problems that the
Level 2 ELT pipeline must clean up:

  * duplicate orders (CDC-style updates: same order_id, later updated_at)
  * future-dated orders (order_date > current_date)
  * NULL totals / amounts
  * messy status values ("SHIPPED", "shiped", "CANCELLED", ...)
  * negative quantities (returns)
  * NULL / padded customer names and emails

Usage:
    python src/generate_data.py            # uses config.yaml profile
    python src/generate_data.py --profile small
    python src/generate_data.py --rows 250_000_000
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import random
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from config import SCALE_PROFILES, human_bytes, load_config
from db import fmt_dur

FIRST_NAMES = [
    "Ava", "Liam", "Sofia", "Noah", "Mia", "Ethan", "Isla", "Lucas", "Zoe", "Mason",
    "Amelia", "Logan", "Layla", "Elijah", "Nora", "Oliver", "Harper", "Carter", "Aria", "Leo",
    "Ella", "Jack", "Chloe", "Henry", "Grace", "Owen", "Ruby", "Wyatt", "Ivy", "Daniel",
    "Hazel", "Samuel", "Luna", "Alexander", "Alice", "James", "Stella", "Benjamin", "Vera", "Elias",
]
LAST_NAMES = [
    "Smith", "Johnson", "Brown", "Garcia", "Miller", "Davis", "Martinez", "Wilson", "Anderson",
    "Taylor", "Thomas", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White",
    "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen",
    "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green", "Adams", "Nelson",
    "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter", "Roberts", "Gomez",
]
COUNTRIES = [
    # (code, country, region, city)
    ("US", "United States", "North America", "New York"), ("CA", "Canada", "North America", "Toronto"),
    ("MX", "Mexico", "North America", "Mexico City"), ("GB", "United Kingdom", "Europe", "London"),
    ("DE", "Germany", "Europe", "Berlin"), ("FR", "France", "Europe", "Paris"),
    ("ES", "Spain", "Europe", "Madrid"), ("IT", "Italy", "Europe", "Milan"),
    ("NL", "Netherlands", "Europe", "Amsterdam"), ("SE", "Sweden", "Europe", "Stockholm"),
    ("PL", "Poland", "Europe", "Warsaw"), ("PT", "Portugal", "Europe", "Lisbon"),
    ("IE", "Ireland", "Europe", "Dublin"), ("CH", "Switzerland", "Europe", "Zurich"),
    ("BR", "Brazil", "South America", "Sao Paulo"), ("AR", "Argentina", "South America", "Buenos Aires"),
    ("CO", "Colombia", "South America", "Bogota"), ("CL", "Chile", "South America", "Santiago"),
    ("PE", "Peru", "South America", "Lima"), ("IN", "India", "Asia", "Mumbai"),
    ("CN", "China", "Asia", "Shanghai"), ("JP", "Japan", "Asia", "Tokyo"),
    ("KR", "South Korea", "Asia", "Seoul"), ("SG", "Singapore", "Asia", "Singapore"),
    ("AE", "UAE", "Middle East", "Dubai"), ("SA", "Saudi Arabia", "Middle East", "Riyadh"),
    ("IL", "Israel", "Middle East", "Tel Aviv"), ("TR", "Turkey", "Middle East", "Istanbul"),
    ("EG", "Egypt", "Africa", "Cairo"), ("NG", "Nigeria", "Africa", "Lagos"),
    ("KE", "Kenya", "Africa", "Nairobi"), ("ZA", "South Africa", "Africa", "Johannesburg"),
    ("MA", "Morocco", "Africa", "Casablanca"), ("AU", "Australia", "Oceania", "Sydney"),
    ("NZ", "New Zealand", "Oceania", "Auckland"), ("ID", "Indonesia", "Asia", "Jakarta"),
    ("TH", "Thailand", "Asia", "Bangkok"), ("VN", "Vietnam", "Asia", "Ho Chi Minh City"),
    ("MY", "Malaysia", "Asia", "Kuala Lumpur"), ("PH", "Philippines", "Asia", "Manila"),
    ("UA", "Ukraine", "Europe", "Kyiv"), ("RO", "Romania", "Europe", "Bucharest"),
    ("CZ", "Czechia", "Europe", "Prague"), ("GR", "Greece", "Europe", "Athens"),
    ("NO", "Norway", "Europe", "Oslo"),
]
PRODUCT_NOUNS = {
    "Electronics": ["Headphones", "Speaker", "Keyboard", "Mouse", "Monitor", "Camera", "Charger",
                    "Smartwatch", "Drone", "Laptop", "Tablet", "Router", "Webcam", "Earbuds"],
    "Home & Kitchen": ["Blender", "Kettle", "Toaster", "Vacuum", "Coffee Maker", "Air Fryer",
                       "Mixer", "Lamp", "Pillow", "Towel Set", "Pan Set", "Water Filter"],
    "Fashion": ["Sneakers", "Jacket", "Jeans", "T-Shirt", "Watch", "Sunglasses", "Backpack",
                "Scarf", "Belt", "Boots", "Dress", "Hoodie"],
    "Sports": ["Yoga Mat", "Dumbbells", "Resistance Band", "Running Shoes", "Bicycle", "Tent",
               "Sleeping Bag", "Water Bottle", "Jump Rope", "Kettlebell"],
    "Books": ["Novel", "Cookbook", "Biography", "Fantasy Saga", "Science Book", "History Book",
              "Self-Help Guide", "Travel Guide", "Poetry Collection", "Textbook"],
    "Beauty": ["Face Cream", "Shampoo", "Perfume", "Lipstick", "Sunscreen", "Serum", "Mascara",
               "Body Lotion", "Hair Dryer", "Face Mask"],
    "Toys": ["Building Blocks", "Puzzle", "Action Figure", "Board Game", "Doll", "RC Car",
             "Plush Toy", "Toy Train", "Art Set", "Drone"],
    "Automotive": ["Car Charger", "Tire", "Car Cover", "Dash Cam", "Seat Cover", "Wiper Blades",
                   "Car Vacuum", "Floor Mats", "Jump Starter", "GPS"],
    "Office": ["Desk Lamp", "Chair", "Notebook", "Pen Set", "Desk Organizer", "Monitor Stand",
               "Whiteboard", "Stapler", "Paper Shredder", "Calendar"],
    "Grocery": ["Coffee Beans", "Olive Oil", "Chocolate", "Tea", "Pasta", "Honey", "Jam",
                "Spice Set", "Granola", "Cereal"],
}
BRANDS = [
    "Nimbus", "Vortex", "Aurora", "Zenith", "Quantum", "Prism", "Orbit", "Cascade", "Ember",
    "Falcon", "Glacier", "Horizon", "Ionix", "Juniper", "Kestrel", "Lumina", "Mystic", "Nova",
    "Onyx", "Pinnacle", "Quartz", "Raven", "Sable", "Titan", "Umbra", "Vector", "Willow",
    "Xenon", "Yonder", "Zephyr",
]
CHANNELS = ["web", "mobile_app", "marketplace", "physical_store", "call_center"]
CURRENCIES = ["USD", "USD", "USD", "USD", "EUR", "GBP"]
PAYMENT_METHODS = ["credit_card", "paypal", "bank_transfer", "gift_card", "apple_pay", "cash"]
STATUSES = ["completed", "pending", "shipped", "cancelled", "refunded"]
EVENT_TYPES = ["view", "add_to_cart", "checkout", "purchase"]

CATEGORIES = list(PRODUCT_NOUNS.keys())
NOUN_LISTS = [PRODUCT_NOUNS[c] for c in CATEGORIES]


def _norm(rng: random.Random) -> float:
    """Standard normal via Box-Muller (DuckDB 1.4+ removed normal_rand())."""
    return (math.sqrt(-2.0 * math.log(1.0 - rng.random()))
            * math.cos(2.0 * math.pi * rng.random()))


def _ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _d(d: date) -> str:
    return d.isoformat()


# ── table generators (mirror the DuckDB gen_sql() logic) ──────────────

def gen_customers(rng: random.Random, n: int, today: date, days: int):
    """n rows: ~2% padded names, ~3% malformed emails, ~2% NULL signup dates."""
    n_f, n_l = len(FIRST_NAMES), len(LAST_NAMES)
    n_c = len(COUNTRIES)
    for i in range(1, n + 1):
        g = rng.randrange(n_c)
        fn = rng.randrange(n_f)
        ln = rng.randrange(n_l)
        seg = rng.randrange(3)
        r = rng.random()
        code, country, region, city = COUNTRIES[g]
        full_name = FIRST_NAMES[fn] + " " + LAST_NAMES[ln]
        if r < 0.02:
            full_name = " " + full_name + " "
        if r > 0.03:
            email = (FIRST_NAMES[fn] + "." + LAST_NAMES[ln] + str(n) + "@mail.com").lower()
        elif r > 0.015:
            email = (FIRST_NAMES[fn] + "." + LAST_NAMES[ln]).lower()   # no domain
        else:
            email = "not-an-email"
        signup = None if r < 0.02 else today - timedelta(days=rng.randrange(days * 3))
        birth = today - timedelta(days=365 * 20 + rng.randrange(365 * 30))
        gender = "M" if rng.random() < 0.5 else "F"
        segment = ("budget" if seg == 0 else "standard" if seg == 1 else "premium")
        active = rng.random() > 0.12
        yield (i, full_name, email, code, country, region, city,
               _d(signup) if signup else "", _d(birth), gender, segment,
               "true" if active else "false")


def gen_products(rng: random.Random, n: int, today: date):
    """n rows: lognormal-ish prices, ~2.5% zero-priced (bad data)."""
    n_b = len(BRANDS)
    for i in range(1, n + 1):
        cat = rng.randrange(10)
        br = rng.randrange(n_b)
        nouns = NOUN_LISTS[cat]
        noun = rng.randrange(len(nouns))   # note: the DuckDB SQL used rnd(14) and
        r = rng.random()                   #       silently produced NULLs on short lists
        sub = nouns[noun]
        name = (BRANDS[br] + " " + sub + " "
                + chr(65 + rng.randrange(26)) + chr(65 + rng.randrange(26)) + "-"
                + str(1000 + rng.randrange(8999)))
        if r < 0.025:
            price = 0.0
        else:
            price = round(max(0.99, math.exp(3.6 + 0.9 * _norm(rng))), 2)
        cost = round(max(0.5, math.exp(3.2 + 0.8 * _norm(rng))), 2)
        weight = round(0.1 + rng.random() * 9.9, 2)
        rating = round(1.0 + rng.random() * 4.0, 1)
        created = today - timedelta(days=rng.randrange(1000))
        yield (i, name, CATEGORIES[cat], sub, BRANDS[br], price, cost,
               weight, rating, _d(created))


def gen_orders(rng: random.Random, n: int, today: date, days: int, n_customers: int):
    """n rows + ~1% CDC-style duplicate update rows; future dates, messy
    statuses, NULL totals. Yields (base_row, dup_row_or_None)."""
    for i in range(1, n + 1):
        customer_id = 1 + rng.randrange(n_customers)
        order_date = today - timedelta(days=int((rng.random() ** 1.6) * days))
        r = rng.random()
        n_items = 1 + rng.randrange(4)
        chan = rng.randrange(3)
        pay = rng.randrange(6)
        st = rng.randrange(5)

        if r < 0.007:                       # ~0.7% future-dated
            order_date = order_date + timedelta(days=rng.randrange(60))
        updated_at = order_date + timedelta(hours=rng.randrange(72))
        if r < 0.007:
            pass
        elif r < 0.010:
            status = STATUSES[st].upper()
        elif r < 0.013:
            status = STATUSES[st].replace("cancel", "cancell")
        elif r < 0.016:
            status = "shiped"
        else:
            status = STATUSES[st]
        channel = CHANNELS[chan]
        currency = CURRENCIES[pay]
        shipping_country = COUNTRIES[rng.randrange(len(COUNTRIES))][0]
        total = None if r < 0.01 else round(rng.random() * 500 + 20, 2)

        row = (i, customer_id, _d(order_date), _ts(updated_at), status, channel,
               currency, shipping_country, "" if total is None else total, n_items)
        dup = None
        if rng.random() < 0.01:             # ~1% duplicate "update" row
            dup_ts = updated_at + timedelta(hours=rng.randrange(24))
            dup_total = total if total is not None else round(rng.random() * 500 + 20, 2)
            dup = (i, customer_id, _d(order_date), _ts(dup_ts), "completed",
                   channel, currency, shipping_country, dup_total, n_items)
        yield row, dup


def gen_order_items(rng: random.Random, n_orders: int, n_products: int):
    """~n_orders*2.2 rows: ~2.5% returns (negative qty), ~1.5% NULL amounts."""
    oid = 0
    for order_id in range(1, n_orders + 1):
        n_items = 1 + rng.randrange(4)
        for _j in range(n_items):
            oid += 1
            product_id = 1 + rng.randrange(n_products)
            if rng.random() < 0.025:
                quantity = -1 - rng.randrange(2)
            else:
                quantity = 1 + rng.randrange(4)
            discount = 0.1 if rng.random() < 0.1 else 0.0
            z = (math.sqrt(-2.0 * math.log(1.0 - rng.random()))
                 * math.cos(2.0 * math.pi * rng.random()))
            unit_price = round(max(0.5, math.exp(3.2 + 0.8 * z)), 2)
            if rng.random() < 0.015:
                amount = None
            else:
                amount = round(quantity * unit_price * (1 - discount), 2)
            yield (oid, order_id, product_id, quantity, unit_price, discount,
                   "" if amount is None else amount, "")  # shipped_at always NULL


def gen_payments(rng: random.Random, n: int, today: date, days: int):
    """n rows + ~0.5% duplicated retry rows. Yields (row, dup_or_None)."""
    for i in range(1, n + 1):
        order_id = 1 + rng.randrange(n)
        payment_date = today - timedelta(days=int((rng.random() ** 1.6) * days))
        amount = round(rng.random() * 500 + 20, 2)
        m = rng.randrange(6)
        r = rng.random()
        method = PAYMENT_METHODS[m]
        if r < 0.03:
            status = "failed"
        elif r < 0.06:
            status = "refunded"
        else:
            status = "completed"
        row = (i, order_id, _d(payment_date), amount, method, status)
        dup = None
        if rng.random() < 0.005:            # ~0.5% retry row, next day
            dup = (i, order_id, _d(payment_date + timedelta(days=1)),
                   amount, method, "completed")
        yield row, dup


def gen_events(rng: random.Random, n: int, now: datetime, n_customers: int, n_products: int):
    """n rows of clickstream events -> JSONL."""
    for i in range(1, n + 1):
        ts = (now
              - timedelta(days=int((rng.random() ** 1.6) * 30))
              - timedelta(seconds=rng.randrange(86400)))
        customer_id = 1 + rng.randrange(n_customers)
        product_id = 1 + rng.randrange(n_products)
        event_type = EVENT_TYPES[rng.randrange(4)]
        session_id = "sess_" + str(100000 + rng.randrange(899999))
        yield {"event_id": i, "event_ts": ts.isoformat(sep=" ", timespec="seconds"),
               "customer_id": customer_id, "product_id": product_id,
               "event_type": event_type, "session_id": session_id}


def write_csv(path: Path, header: list[str], rows) -> int:
    n = 0
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        for r in rows:
            w.writerow(r)
            n += 1
    return n


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate synthetic e-commerce raw data")
    ap.add_argument("--profile", choices=list(SCALE_PROFILES), help="Override scale profile")
    ap.add_argument("--rows", type=int, help="Override number of orders directly")
    args = ap.parse_args()

    cfg = load_config()
    if args.profile:
        cfg.scale["profile"] = args.profile

    n_orders = args.rows or cfg.n_orders
    n_customers = max(1, int(n_orders / cfg.scale["orders_per_customer"]))
    n_products = int(cfg.scale["n_products"])
    days = int(cfg.scale["history_days"])
    n_payments = n_orders
    n_events = min(n_orders, 20_000_000)

    raw = Path(cfg.data["raw"])
    raw.mkdir(parents=True, exist_ok=True)
    today = date.today()
    now = datetime.now()

    print(f"Generating data for {n_orders:,} orders "
          f"(profile={cfg.scale['profile']}, seed={cfg.seed})")
    print(f"Raw output: {raw}\n")

    t0 = time.perf_counter()
    rng = random.Random(cfg.seed)

    jobs = [
        ("customers.csv", ["customer_id", "full_name", "email", "country_code", "country",
                           "region", "city", "signup_date", "birth_date", "gender",
                           "segment", "is_active"],
         gen_customers(rng, n_customers, today, days)),
        ("products.csv", ["product_id", "name", "category", "subcategory", "brand",
                          "unit_price", "cost", "weight_kg", "rating", "created_at"],
         gen_products(rng, n_products, today)),
        ("orders.csv", ["order_id", "customer_id", "order_date", "updated_at", "status",
                        "channel", "currency", "shipping_country", "total_amount", "n_items"],
         _flat_orders(gen_orders(rng, n_orders, today, days, n_customers))),
        ("order_items.csv", ["order_item_id", "order_id", "product_id", "quantity",
                             "unit_price", "discount_rate", "amount", "shipped_at"],
         gen_order_items(rng, n_orders, n_products)),
        ("payments.csv", ["payment_id", "order_id", "payment_date", "amount", "method",
                          "status"],
         _flat_payments(gen_payments(rng, n_payments, today, days))),
    ]
    for fname, header, rows in jobs:
        out = raw / fname
        st = time.perf_counter()
        n = write_csv(out, header, rows)
        print(f"  wrote {fname:<20} rows={n:>12,}  size={human_bytes(out.stat().st_size)}  "
              f"({fmt_dur(time.perf_counter() - st)})")

    # JSONL events
    events_path = raw / "events.jsonl"
    st = time.perf_counter()
    n = 0
    with open(events_path, "w", encoding="utf-8") as fh:
        for ev in gen_events(rng, n_events, now, n_customers, n_products):
            fh.write(json.dumps(ev) + "\n")
            n += 1
    print(f"  wrote {'events.jsonl':<20} rows={n:>12,}  "
          f"size={human_bytes(events_path.stat().st_size)}  "
          f"({fmt_dur(time.perf_counter() - st)})")

    elapsed = time.perf_counter() - t0
    total = sum(p.stat().st_size for p in raw.iterdir())
    print(f"\nDone in {fmt_dur(elapsed)}. Total raw size: {human_bytes(total)}")


# small helpers: expand (row, dup_row) pairs — base rows first, dup rows after

def _flat_orders(pairs):
    """Expand (row, dup_row) pairs: base rows first, then their dup rows."""
    base, dups = [], []
    for row, dup in pairs:
        base.append(row)
        if dup is not None:
            dups.append(dup)
    yield from base
    yield from dups


def _flat_payments(pairs):
    base, dups = [], []
    for row, dup in pairs:
        base.append(row)
        if dup is not None:
            dups.append(dup)
    yield from base
    yield from dups


if __name__ == "__main__":
    main()
