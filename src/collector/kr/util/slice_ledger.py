"""Per-slice completion ledger for backfills that outlive a single run (L-1).

Every collector here already skips work it has done. Two of those mechanisms
stop working once a backfill gets large enough:

**"the slice has rows, so it is done" is not completion.** If a response is
half-written and the process dies, that slice has rows forever and every later
run skips it. Nothing repairs it, and nothing reports it. The fix is to record
what the response *carried* alongside what storage *wrote*, and to call a slice
done only when the two agree.

**``ingestion_runs.params`` cannot hold the answer for a multi-day backfill.**
The no-data tombstone list is capped per run and only the newest runs are read
back, so a job spanning three days cannot reconstruct its own progress. N6 is
~84,000 calls over about three days — exactly the case that breaks.

So the state moves into ``collection_slice_state``, keyed by
``(source, endpoint, slice_key)``. This module is the only thing collectors
need to touch: :meth:`SliceLedger.plan` decides what to fetch, and the three
``record_*`` methods report back.

Build it once and share it. A ledger written per work package is a ledger with
per-package bugs.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import timedelta

from collector.kr.domain.enums import SliceStatus, Source
from collector.kr.domain.models import CollectionSliceState
from collector.kr.ports.storage import Storage
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)

#: How long a ``no_data`` verdict stands before the slice is asked again.
#: "Upstream has nothing" is a statement about when it was asked: a suspended
#: ticker resumes, and a company that filed nothing for a period may file a
#: correction later. ``success`` has no equivalent — it does not expire.
DEFAULT_NO_DATA_TTL_DAYS = 30


@dataclass(slots=True)
class SlicePlan:
    """What a run should collect, and why the rest was skipped.

    The skip reasons are separated because they mean different things to an
    operator: ``skipped_complete`` is healthy, while a large
    ``skipped_no_data`` on a fresh endpoint usually means the request keys are
    wrong rather than that upstream is empty.
    """

    pending: list[str] = field(default_factory=list)
    skipped_complete: list[str] = field(default_factory=list)
    skipped_no_data: list[str] = field(default_factory=list)
    retrying: list[str] = field(default_factory=list)

    @property
    def skipped(self) -> int:
        """Total slices this run will not request."""
        return len(self.skipped_complete) + len(self.skipped_no_data)

    def as_counts(self) -> dict[str, int]:
        """Counters for ``ingestion_runs.counts``."""
        return {
            "slices_pending": len(self.pending),
            "slices_skipped_complete": len(self.skipped_complete),
            "slices_skipped_no_data": len(self.skipped_no_data),
            "slices_retrying": len(self.retrying),
        }


class SliceLedger:
    """Reads and writes one ``(source, endpoint)`` slice ledger.

    Args:
        storage: Storage holding ``collection_slice_state``.
        source: Upstream system.
        endpoint: Endpoint name within that source. Two collectors reading the
            same source must not share one, or they collide on slice keys.
        no_data_ttl_days: How long a ``no_data`` verdict stands. ``None`` keeps it
            indefinitely.
    """

    def __init__(
        self,
        storage: Storage,
        *,
        source: Source,
        endpoint: str,
        no_data_ttl_days: int | None = DEFAULT_NO_DATA_TTL_DAYS,
    ) -> None:
        self._storage = storage
        self._source = source
        self._endpoint = endpoint
        # ``None`` is reserved for immutable historical slices: once an old
        # report has answered no-data, normal reruns must not spend quota asking
        # the identical question again. ``force=True`` still bypasses it.
        self._no_data_ttl_days = (
            None if no_data_ttl_days is None else max(0, no_data_ttl_days)
        )
        self._pending_writes: list[CollectionSliceState] = []

    def plan(self, slice_keys: Sequence[str], *, force: bool = False) -> SlicePlan:
        """Split *slice_keys* into what to collect and what to skip.

        Args:
            slice_keys: Every slice the run would collect if nothing were
                stored. Order is preserved in the result.
            force: Collect everything, ignoring the ledger. The ledger is still
                written, so a forced run repairs stale rows rather than
                bypassing them permanently.

        Returns:
            A :class:`SlicePlan`.
        """
        plan = SlicePlan()
        if force:
            plan.pending = list(slice_keys)
            return plan

        states = self._storage.get_collection_slice_states(
            self._source, self._endpoint, list(slice_keys)
        )
        cutoff = (
            None
            if self._no_data_ttl_days is None
            else now_kst() - timedelta(days=self._no_data_ttl_days)
        )

        for key in slice_keys:
            state = states.get(key)
            if state is None:
                plan.pending.append(key)
                continue

            if state.status is SliceStatus.SUCCESS:
                if state.is_complete:
                    plan.skipped_complete.append(key)
                else:
                    # Recorded as success but the row counts disagree — the
                    # half-written slice this ledger exists to catch.
                    logger.warning(
                        "slice %s/%s %s claims success with %s of %s rows; re-collecting",
                        self._source.value,
                        self._endpoint,
                        key,
                        state.actual_rows,
                        state.expected_rows,
                    )
                    plan.retrying.append(key)
                    plan.pending.append(key)
                continue

            if state.status is SliceStatus.NO_DATA:
                if cutoff is None or (
                    state.updated_at is not None and state.updated_at >= cutoff
                ):
                    plan.skipped_no_data.append(key)
                else:
                    plan.pending.append(key)
                continue

            # running or failed. A `running` row means a previous process was
            # killed before reporting; treating it as done would leave a slice
            # nothing ever revisits.
            plan.retrying.append(key)
            plan.pending.append(key)

        return plan

    def record_success(self, slice_key: str, *, expected_rows: int, actual_rows: int) -> None:
        """Record a collected slice.

        A mismatch between the two counts is stored as-is and read back as
        incomplete, so the next run re-collects rather than trusting the
        status word.
        """
        status = SliceStatus.SUCCESS
        if expected_rows != actual_rows:
            logger.warning(
                "slice %s/%s %s wrote %d of %d rows; recording as failed",
                self._source.value,
                self._endpoint,
                slice_key,
                actual_rows,
                expected_rows,
            )
            status = SliceStatus.FAILED
        self._queue(
            slice_key,
            status,
            expected_rows=expected_rows,
            actual_rows=actual_rows,
            last_error=(
                None
                if status is SliceStatus.SUCCESS
                else f"row count mismatch: wrote {actual_rows} of {expected_rows}"
            ),
        )

    def record_no_data(self, slice_key: str) -> None:
        """Record that upstream really has nothing for this slice."""
        self._queue(slice_key, SliceStatus.NO_DATA, expected_rows=0, actual_rows=0)

    def record_failure(self, slice_key: str, error: str) -> None:
        """Record a failed attempt, keeping the slice pending for later runs."""
        self._queue(slice_key, SliceStatus.FAILED, last_error=error)

    def record_running(self, slice_keys: Iterable[str]) -> None:
        """Claim slices before a long fetch so a crash is visible afterwards.

        Optional: a collector that flushes per slice does not need it. It earns
        its keep on runs long enough that "which slice was in flight when the
        host went down" is a question someone asks.
        """
        for key in slice_keys:
            self._queue(key, SliceStatus.RUNNING)

    def flush(self) -> int:
        """Write queued states and return how many rows were written.

        Batched because a per-slice write on an 84,000-call backfill is 84,000
        extra round trips. Callers should flush periodically, not only at the
        end — the point of the ledger is surviving an interrupted run.
        """
        if not self._pending_writes:
            return 0
        states, self._pending_writes = self._pending_writes, []
        result = self._storage.upsert_collection_slice_states(states)
        return result.updated

    @property
    def pending_write_count(self) -> int:
        """Queued states not yet flushed."""
        return len(self._pending_writes)

    def _queue(
        self,
        slice_key: str,
        status: SliceStatus,
        *,
        expected_rows: int | None = None,
        actual_rows: int | None = None,
        last_error: str | None = None,
    ) -> None:
        self._pending_writes.append(
            CollectionSliceState(
                source=self._source,
                endpoint=self._endpoint,
                slice_key=slice_key,
                status=status,
                expected_rows=expected_rows,
                actual_rows=actual_rows,
                last_error=last_error,
                updated_at=now_kst(),
            )
        )
