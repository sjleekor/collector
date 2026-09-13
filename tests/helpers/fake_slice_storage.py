"""Reusable in-memory storage methods for collection-slice ledger tests."""

from __future__ import annotations

from collector.kr.domain.enums import Source
from collector.kr.domain.models import CollectionSliceState, UpsertResult


class FakeSliceStorageMixin:
    """Provide the two ``collection_slice_state`` storage operations."""

    def __init__(self) -> None:
        self.slice_states: dict[tuple[Source, str, str], CollectionSliceState] = {}

    def get_collection_slice_states(
        self,
        source: Source,
        endpoint: str,
        slice_keys: list[str] | None = None,
    ) -> dict[str, CollectionSliceState]:
        allowed = None if slice_keys is None else set(slice_keys)
        return {
            slice_key: state
            for (state_source, state_endpoint, slice_key), state in self.slice_states.items()
            if state_source is source
            and state_endpoint == endpoint
            and (allowed is None or slice_key in allowed)
        }

    def upsert_collection_slice_states(
        self, states: list[CollectionSliceState]
    ) -> UpsertResult:
        for state in states:
            self.slice_states[(state.source, state.endpoint, state.slice_key)] = state
        return UpsertResult(updated=len(states))
