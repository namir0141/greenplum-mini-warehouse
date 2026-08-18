"""Shared configuration loading for the Greenplum mini data warehouse."""
from __future__ import annotations

import os
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent

# Map scale profile -> number of orders
SCALE_PROFILES = {
    "smoke": 100_000,
    "small": 1_000_000,
    "medium": 10_000_000,
    "large": 100_000_000,
}


class Config:
    def __init__(self, path: Path = ROOT / "config.yaml"):
        with open(path, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)

        self.data = raw["data"]
        self.scale = raw["scale"]
        self.benchmark = raw["benchmark"]
        self.database = raw["database"]
        self.seed = int(raw.get("seed", 42))

        # Resolve every path against the project root.
        for key in ("raw", "bronze", "silver", "gold", "incremental"):
            p = Path(self.data[key])
            if not p.is_absolute():
                p = ROOT / p
            self.data[key] = p
        manifest = Path(self.data["manifest"])
        if not manifest.is_absolute():
            manifest = ROOT / manifest
        self.data["manifest"] = manifest

        for layer in ("raw", "bronze", "silver", "gold", "incremental"):
            self.data[layer].mkdir(parents=True, exist_ok=True)

    @property
    def n_orders(self) -> int:
        """Number of orders for the configured scale profile."""
        return SCALE_PROFILES[self.scale["profile"]]

    @property
    def dsn(self) -> dict:
        """Connection parameters, preferring standard PG* env vars."""
        db = self.database
        return {
            "host": os.environ.get("PGHOST", db.get("host", "localhost")),
            "port": int(os.environ.get("PGPORT", db.get("port", 5432))),
            "dbname": os.environ.get("PGDATABASE", db.get("dbname", "postgres")),
            "user": os.environ.get("PGUSER", db.get("user", "gpadmin")),
            "password": os.environ.get("PGPASSWORD", db.get("password", "")),
        }


def load_config() -> Config:
    return Config()


def human_bytes(n: float) -> str:
    """Format a byte count for humans."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"
