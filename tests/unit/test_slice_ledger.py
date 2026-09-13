"""The per-slice completion ledger (L-1).

Most of these tests pin behaviour that the mechanisms this replaces get wrong:
a half-written slice must not read as done, a killed run must leave work
behind rather than a permanent gap, and a "no data" verdict must expire while
a success does not.
"""

from __future__ import annotations

from datetime import timedelta

from collector.kr.domain.enums import SliceStatus, Source
from collector.kr.domain.models import CollectionSliceState, UpsertResult
from collector.kr.util.slice_ledger import SliceLedger
from collector.kr.util.time import now_kst

ENDPOINT = "empSttus"
SOURCE = Source.OPENDART


class _FakeStorage:
    def __init__(self, states: list[CollectionSliceState] | None = None) -> None:
        self._states = {s.slice_key: s for s in (states or [])}
        self.written: list[CollectionSliceState] = []
        self.read_calls: list[tuple[Source, str, list[str] | None]] = []

    def get_collection_slice_states(
        self,
        source: Source,
        endpoint: str,
        slice_keys: list[str] | None = None,
    ) -> dict[str, CollectionSliceState]:
        self.read_calls.append((source, endpoint, slice_keys))
        if slice_keys is None:
            return dict(self._states)
        return {k: v for k, v in self._states.items() if k in set(slice_keys)}

    def upsert_collection_slice_states(self, states: list[CollectionSliceState]) -> UpsertResult:
        self.written.extend(states)
        return UpsertResult(updated=len(states))


def _state(key: str, status: SliceStatus, **kwargs) -> CollectionSliceState:
    kwargs.setdefault("updated_at", now_kst())
    return CollectionSliceState(
        source=SOURCE, endpoint=ENDPOINT, slice_key=key, status=status, **kwargs
    )


def _ledger(storage: _FakeStorage, **kwargs) -> SliceLedger:
    return SliceLedger(storage, source=SOURCE, endpoint=ENDPOINT, **kwargs)


# --------------------------------------------------------------------------
# planning
# --------------------------------------------------------------------------


def test_an_unseen_slice_is_pending() -> None:
    plan = _ledger(_FakeStorage()).plan(["a", "b"])

    assert plan.pending == ["a", "b"]
    assert plan.skipped == 0


def test_a_reconciled_success_is_skipped() -> None:
    storage = _FakeStorage([_state("a", SliceStatus.SUCCESS, expected_rows=10, actual_rows=10)])

    plan = _ledger(storage).plan(["a", "b"])

    assert plan.pending == ["b"]
    assert plan.skipped_complete == ["a"]


def test_a_success_whose_row_counts_disagree_is_re_collected() -> None:
    # The whole reason this ledger exists. "The slice has rows" is not
    # completion: a response half-written before a crash otherwise leaves a
    # slice every later run skips and nothing repairs.
    storage = _FakeStorage([_state("a", SliceStatus.SUCCESS, expected_rows=100, actual_rows=40)])

    plan = _ledger(storage).plan(["a"])

    assert plan.pending == ["a"]
    assert plan.retrying == ["a"]
    assert plan.skipped_complete == []


def test_a_success_with_no_expected_count_is_taken_at_its_word() -> None:
    # Not every caller knows what the response carried; demanding a count it
    # cannot supply would make the slice permanently pending.
    storage = _FakeStorage([_state("a", SliceStatus.SUCCESS)])

    assert _ledger(storage).plan(["a"]).skipped_complete == ["a"]


def test_a_running_slice_is_retried_not_treated_as_done() -> None:
    # A `running` row means a process was killed before reporting. Reading it
    # as done leaves a slice nothing ever revisits.
    storage = _FakeStorage([_state("a", SliceStatus.RUNNING)])

    plan = _ledger(storage).plan(["a"])

    assert plan.pending == ["a"]
    assert plan.retrying == ["a"]


def test_a_failed_slice_stays_pending() -> None:
    storage = _FakeStorage([_state("a", SliceStatus.FAILED, last_error="boom")])

    assert _ledger(storage).plan(["a"]).pending == ["a"]


def test_a_fresh_no_data_verdict_is_skipped() -> None:
    storage = _FakeStorage([_state("a", SliceStatus.NO_DATA)])

    plan = _ledger(storage, no_data_ttl_days=30).plan(["a"])

    assert plan.pending == []
    assert plan.skipped_no_data == ["a"]


def test_a_stale_no_data_verdict_expires() -> None:
    # "Upstream has nothing" is a statement about when it was asked. A
    # suspended ticker resumes; a period with no filing can get a correction.
    storage = _FakeStorage(
        [_state("a", SliceStatus.NO_DATA, updated_at=now_kst() - timedelta(days=31))]
    )

    plan = _ledger(storage, no_data_ttl_days=30).plan(["a"])

    assert plan.pending == ["a"]
    assert plan.skipped_no_data == []


def test_no_data_never_expires_when_ttl_is_none() -> None:
    storage = _FakeStorage(
        [_state("a", SliceStatus.NO_DATA, updated_at=now_kst() - timedelta(days=3650))]
    )

    plan = _ledger(storage, no_data_ttl_days=None).plan(["a"])

    assert plan.pending == []
    assert plan.skipped_no_data == ["a"]


def test_success_never_expires_the_way_no_data_does() -> None:
    storage = _FakeStorage(
        [
            _state(
                "a",
                SliceStatus.SUCCESS,
                expected_rows=1,
                actual_rows=1,
                updated_at=now_kst() - timedelta(days=3650),
            )
        ]
    )

    assert _ledger(storage, no_data_ttl_days=30).plan(["a"]).skipped_complete == ["a"]


def test_force_collects_everything_without_reading_the_ledger() -> None:
    storage = _FakeStorage([_state("a", SliceStatus.SUCCESS, expected_rows=1, actual_rows=1)])

    plan = _ledger(storage).plan(["a", "b"], force=True)

    assert plan.pending == ["a", "b"]
    assert storage.read_calls == []


def test_the_plan_preserves_input_order() -> None:
    storage = _FakeStorage([_state("b", SliceStatus.SUCCESS, expected_rows=1, actual_rows=1)])

    assert _ledger(storage).plan(["c", "b", "a"]).pending == ["c", "a"]


def test_plan_counts_are_ready_for_the_run_record() -> None:
    storage = _FakeStorage(
        [
            _state("a", SliceStatus.SUCCESS, expected_rows=1, actual_rows=1),
            _state("b", SliceStatus.NO_DATA),
            _state("c", SliceStatus.RUNNING),
        ]
    )

    counts = _ledger(storage).plan(["a", "b", "c", "d"]).as_counts()

    assert counts == {
        "slices_pending": 2,
        "slices_skipped_complete": 1,
        "slices_skipped_no_data": 1,
        "slices_retrying": 1,
    }


# --------------------------------------------------------------------------
# recording
# --------------------------------------------------------------------------


def test_a_matching_row_count_records_success() -> None:
    storage = _FakeStorage()
    ledger = _ledger(storage)

    ledger.record_success("a", expected_rows=10, actual_rows=10)
    assert ledger.flush() == 1

    (written,) = storage.written
    assert written.status is SliceStatus.SUCCESS
    assert (written.expected_rows, written.actual_rows) == (10, 10)
    assert written.last_error is None


def test_a_row_count_mismatch_is_recorded_as_failed_not_success() -> None:
    # Recording it as success with mismatched counts would rely on every
    # future reader re-deriving completeness. It is a failure; say so.
    storage = _FakeStorage()
    ledger = _ledger(storage)

    ledger.record_success("a", expected_rows=100, actual_rows=40)
    ledger.flush()

    (written,) = storage.written
    assert written.status is SliceStatus.FAILED
    assert written.last_error is not None
    assert "40" in written.last_error and "100" in written.last_error


def test_no_data_is_recorded_with_zero_rows() -> None:
    storage = _FakeStorage()
    ledger = _ledger(storage)

    ledger.record_no_data("a")
    ledger.flush()

    (written,) = storage.written
    assert written.status is SliceStatus.NO_DATA
    assert (written.expected_rows, written.actual_rows) == (0, 0)


def test_a_failure_keeps_its_message() -> None:
    storage = _FakeStorage()
    ledger = _ledger(storage)

    ledger.record_failure("a", "HTTP 500")
    ledger.flush()

    (written,) = storage.written
    assert written.status is SliceStatus.FAILED
    assert written.last_error == "HTTP 500"


def test_writes_are_batched_until_flushed() -> None:
    # A per-slice write on an 84,000-call backfill is 84,000 extra round trips.
    storage = _FakeStorage()
    ledger = _ledger(storage)

    ledger.record_success("a", expected_rows=1, actual_rows=1)
    ledger.record_no_data("b")
    assert storage.written == []
    assert ledger.pending_write_count == 2

    assert ledger.flush() == 2
    assert len(storage.written) == 2
    assert ledger.pending_write_count == 0


def test_flushing_nothing_does_not_hit_storage() -> None:
    storage = _FakeStorage()

    assert _ledger(storage).flush() == 0
    assert storage.written == []


def test_recording_running_claims_slices_before_the_fetch() -> None:
    storage = _FakeStorage()
    ledger = _ledger(storage)

    ledger.record_running(["a", "b"])
    ledger.flush()

    assert [s.status for s in storage.written] == [SliceStatus.RUNNING, SliceStatus.RUNNING]


def test_a_forced_run_still_writes_the_ledger() -> None:
    # Otherwise --force would bypass the ledger permanently instead of
    # repairing the stale rows that made someone reach for it.
    storage = _FakeStorage([_state("a", SliceStatus.SUCCESS, expected_rows=5, actual_rows=2)])
    ledger = _ledger(storage)

    ledger.plan(["a"], force=True)
    ledger.record_success("a", expected_rows=5, actual_rows=5)
    ledger.flush()

    (written,) = storage.written
    assert written.status is SliceStatus.SUCCESS
    assert written.actual_rows == 5
