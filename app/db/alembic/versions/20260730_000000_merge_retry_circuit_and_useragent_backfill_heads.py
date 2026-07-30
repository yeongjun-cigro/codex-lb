"""merge http-bridge retry-circuit and useragent-backfill heads

Revision ID: 20260730_000000_merge_retry_circuit_and_useragent_backfill_heads
Revises:
- 20260717_000000_add_http_bridge_retry_circuits
- 20260722_000000_backfill_request_log_useragent_families
Create Date: 2026-07-30 00:00:00.000000
"""

from __future__ import annotations

revision = "20260730_000000_merge_retry_circuit_and_useragent_backfill_heads"
down_revision = (
    "20260717_000000_add_http_bridge_retry_circuits",
    "20260722_000000_backfill_request_log_useragent_families",
)
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
