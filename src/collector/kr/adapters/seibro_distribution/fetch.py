"""SEIBro 창 받기. 구간마다 ``LIST_CNT``와 고유 행 수가 같아야 완료입니다."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date

from collector.kr.adapters.seibro_distribution.client import (
    SeibroClient,
    SeibroMalformedResponseError,
    SeibroRawResponse,
    SeibroRequestError,
)
from collector.kr.adapters.seibro_distribution.parser import (
    DistributionRow,
    parse_count,
    parse_rows,
)
from collector.kr.adapters.seibro_distribution.request import (
    PAGE_SIZE,
    build_count_request,
    build_list_request,
)
from collector.kr.adapters.seibro_distribution.window import year_chunks

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ChunkResult:
    """연도 구간 하나의 기대·실제 행 수."""

    start: date
    end: date
    expected: int | None = None
    actual: int = 0
    complete: bool = False
    error: str = ""


@dataclass(slots=True)
class WindowResult:
    """창 받기 결과. ``complete``가 False면 ``absent`` 판단에 쓰지 않습니다."""

    start: date
    end: date
    rows: list[DistributionRow] = field(default_factory=list)
    raw_responses: list[SeibroRawResponse] = field(default_factory=list)
    chunks: list[ChunkResult] = field(default_factory=list)
    complete: bool = False


def _fetch_chunk(
    client: SeibroClient, lo: date, hi: date, result: WindowResult, chunk: ChunkResult
) -> None:
    count_resp = client.post(build_count_request(lo, hi), range_start=lo, range_end=hi)
    result.raw_responses.append(count_resp)
    chunk.expected = parse_count(count_resp.body)

    unique: dict[tuple, DistributionRow] = {}
    pages = (chunk.expected + PAGE_SIZE - 1) // PAGE_SIZE
    for page in range(1, pages + 1):
        resp = client.post(
            build_list_request(lo, hi, page), range_start=lo, range_end=hi, page=page
        )
        result.raw_responses.append(resp)
        for row in parse_rows(resp.body):
            unique[row.key] = row
    chunk.actual = len(unique)
    result.rows.extend(unique.values())
    chunk.complete = chunk.actual == chunk.expected
    if not chunk.complete:
        chunk.error = f"LIST_CNT {chunk.expected} != unique rows {chunk.actual}"


def fetch_window(client: SeibroClient, start: date, end: date) -> WindowResult:
    """``start``~``end``를 연도 구간별로 받습니다.

    페이지 하나라도 실패하면 창 전체를 미완료로 두고 거기서 멈춥니다. 이미 받은
    원문 응답은 ``raw_responses``에 남습니다. 구간 사이에 걸치는 행은 없으므로
    (기준일이 한 연도에 속함) 구간별 중복 제거로 충분합니다.
    """
    result = WindowResult(start=start, end=end)
    all_ok = True
    for lo, hi in year_chunks(start, end):
        chunk = ChunkResult(start=lo, end=hi)
        result.chunks.append(chunk)
        try:
            _fetch_chunk(client, lo, hi, result, chunk)
        except (SeibroRequestError, SeibroMalformedResponseError) as exc:
            chunk.error = f"{type(exc).__name__}: {exc}"
            logger.warning("SEIBro chunk %s~%s failed: %s", lo, hi, chunk.error)
            all_ok = False
            break
        if not chunk.complete:
            all_ok = False
    result.complete = all_ok and bool(result.chunks)
    return result
