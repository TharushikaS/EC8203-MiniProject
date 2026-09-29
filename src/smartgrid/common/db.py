"""PostgreSQL helpers (serving store) with retry-on-startup and idempotent upserts."""
from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Iterable, Sequence

import psycopg2
import psycopg2.extras

from smartgrid.common.config import Settings, get_settings


def connect(settings: Settings | None = None, retries: int = 30, delay_s: float = 2.0):
    settings = settings or get_settings()
    last_exc: Exception | None = None
    for _ in range(retries):
        try:
            return psycopg2.connect(settings.postgres_dsn, connect_timeout=5)
        except psycopg2.OperationalError as exc:   # DB still starting up
            last_exc = exc
            time.sleep(delay_s)
    raise RuntimeError(f"PostgreSQL unreachable: {last_exc}")


@contextmanager
def transaction(settings: Settings | None = None):
    """Yield a cursor inside a transaction; commit on success, roll back on error."""
    conn = connect(settings)
    try:
        with conn:
            with conn.cursor() as cur:
                yield cur
    finally:
        conn.close()


def upsert(cur, table: str, columns: Sequence[str], rows: Iterable[Sequence],
           conflict_cols: Sequence[str], update_cols: Sequence[str] | None = None,
           page_size: int = 500) -> int:
    """INSERT ... ON CONFLICT DO UPDATE. Re-running with the same rows is a no-op
    (idempotent), which makes Spark foreachBatch replays after a failure safe."""
    rows = list(rows)
    if not rows:
        return 0
    update_cols = [c for c in (update_cols if update_cols is not None else columns) if c not in conflict_cols]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    action = f"DO UPDATE SET {set_clause}" if set_clause else "DO NOTHING"
    sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s "
        f"ON CONFLICT ({', '.join(conflict_cols)}) {action}"
    )
    psycopg2.extras.execute_values(cur, sql, rows, page_size=page_size)
    return len(rows)
