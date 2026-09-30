# Copyright 2023-2026 Airbus, CS Group
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Shared instance registry, used to aggregate GET /dpr/processes across every running rs-dpr-service instance.

Context: a single rs-dpr-service deployment can be installed several times (several Helm releases), each
instance exposing a different subset of processors via DPR_ENABLED_PROCESSORS. Every instance shares the
same PostgreSQL database (see jobs_table.py). We reuse that database as a lightweight service registry:

- On startup, each instance upserts its own row (instance id + the processor ids it exposes).
- A background heartbeat task periodically refreshes 'last_seen' so peers know the instance is still alive.
- On shutdown, the instance removes its own row on a best-effort basis.
- Readers (GET /dpr/processes) select every row that isn't stale and merge the processor ids, so any
  instance can answer with the full, near-real-time list of processors exposed by the whole fleet, not
  just the processors it exposes locally.

No separate cleanup job is required: a row from an instance that crashed without a clean shutdown simply
stops being refreshed and is filtered out by readers once it becomes stale (see 'last_seen' below).
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta, timezone

from sqlalchemy import Column, DateTime, String, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from rs_dpr_service.jobs_table import Base

# pylint: disable=too-few-public-methods
# mypy: ignore-errors
# Ignore pylint and mypy false positive errors on sqlalchemy


class InstanceRegistry(Base):
    """One row per running rs-dpr-service instance (pod)."""

    __tablename__ = "dpr_instances"

    # Stable identifier for the instance. In cluster mode this is the pod name (HOSTNAME env var).
    instance_id = Column(String, primary_key=True, unique=True, index=True)

    # Comma-separated processor ids exposed by this instance, e.g. "s1_l0,s1_ard".
    # Matches the format of the DPR_ENABLED_PROCESSORS environment variable.
    processors = Column(String, nullable=False)

    # Refreshed on every heartbeat. Rows older than DPR_REGISTRY_STALE_AFTER_SECONDS are ignored by readers.
    last_seen = Column(
        DateTime,
        server_default=func.now(),  # pylint: disable=not-callable
        onupdate=func.now(),  # pylint: disable=not-callable
        server_onupdate=func.now(),  # pylint: disable=not-callable
        nullable=False,
        index=True,
    )


def utc_now() -> datetime:
    """Current UTC time as a naive datetime, to match the (timezone-less) 'last_seen' column type."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def register_instance(engine: Engine, instance_id: str, processor_ids: Sequence[str]) -> None:
    """Create or refresh (upsert) this instance's row in the registry. Used at startup and on every heartbeat."""
    values = {"instance_id": instance_id, "processors": ",".join(processor_ids), "last_seen": utc_now()}
    upsert = pg_insert(InstanceRegistry).values(**values)
    upsert = upsert.on_conflict_do_update(
        index_elements=[InstanceRegistry.instance_id],
        set_={"processors": upsert.excluded.processors, "last_seen": upsert.excluded.last_seen},
    )
    with Session(engine) as session:
        session.execute(upsert)
        session.commit()


def unregister_instance(engine: Engine, instance_id: str) -> None:
    """Best-effort removal of this instance's row, called on a graceful shutdown."""
    with Session(engine) as session:
        session.query(InstanceRegistry).filter(InstanceRegistry.instance_id == instance_id).delete()
        session.commit()


def list_processor_ids(engine: Engine, stale_after_seconds: int) -> list[str]:
    """Return the sorted, deduplicated union of processor ids from every instance that is not stale."""
    cutoff = utc_now() - timedelta(seconds=stale_after_seconds)
    with Session(engine) as session:
        rows = session.query(InstanceRegistry).filter(InstanceRegistry.last_seen >= cutoff).all()
    processor_ids: set[str] = set()
    for row in rows:
        processor_ids.update(processor_id.strip() for processor_id in row.processors.split(",") if processor_id.strip())
    return sorted(processor_ids)
