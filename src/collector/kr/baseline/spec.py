"""표 설명(``TableSpec``)과 관측 행의 해시 — 표 열 구성을 여기서 박지 않는다.

**왜 일반화했나.** ETF 19필드·채권지수·분배금은 파서가 따로 만든다. 저장 계약
(관측 순번·요청 기록·완료 manifest)은 열이 무엇이든 같아야 하므로, 표마다
"키가 무엇이고 원천 칸이 무엇인가"만 받는다.

**왜 ``year`` 컬럼이 없나.** 경로의 ``year=YYYY``와 같은 이름의 데이터 컬럼이
있으면 읽을 때 한쪽이 다른 쪽을 덮는다(한국에서 ``source``로 겪은 사고). 그래서
``year``는 경로에만 두고 ``TableSpec``이 그 이름을 쓰지 못하게 막는다.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

#: 관측 표에 붙는 출처 칸. 원천 칸과 이름이 겹치면 안 된다.
PROVENANCE_COLUMNS: tuple[str, ...] = (
    "fetched_at",
    "fetched_at_basis",
    "obs_seq",
    "obs_kind",
    "source_run",
    "run_id",
    "attempt",
    "raw_sha256",
    "raw_path",
    "row_hash",
)

#: ``last_confirmed_at``·``first_fetched_at``은 관측 표에 두지 않는다(R05).
#: 같은 응답을 확인할 때마다 최초 관측본 행을 고쳐야 하기 때문이다.
FORBIDDEN_COLUMNS: frozenset[str] = frozenset({"year", "last_confirmed_at", "first_fetched_at"})

OBS_VALUE = "value"
OBS_ABSENT = "absent"
FETCHED_AT_BASES = ("response", "file_mtime")

_ARROW_TYPES: dict[str, pa.DataType] = {
    "float64": pa.float64(),
    "int64": pa.int64(),
    "date32": pa.date32(),
    "bool": pa.bool_(),
    "string": pa.string(),
}

_SEP = "\x1f"  # 단위 구분자. 원천 값에 나오지 않는다.
_NULL = "\x00N"  # None 표시. 빈 문자열("")과 구분한다.


@dataclass(frozen=True)
class TableSpec:
    """관측 표 하나의 설명.

    Args:
        name: 표 이름이자 디렉터리 이름. DB 표와 같은 이름을 쓰지 않는다.
        key_columns: 업무 키. 값은 문자열이다(``0184E0`` 같은 코드가 있다).
        source_columns: 원천 칸. 원문 문자열 그대로이고 ``row_hash``의 입력이다.
            **이 순서가 해시 순서**라서 바꾸면 기존 관측과 전부 달라 보인다.
        parsed_columns: 원문에서 바꾼 칸의 ``{이름: 형식}``. 형식은 ``float64``·
            ``int64``·``date32``·``bool``·``string``. 해시에는 안 들어간다 —
            같은 원문에서 나오는 파생값이라서다.
        date_column: ``year=`` 파티션을 정하는 키 칸. 값은 ``YYYY…``로 시작한다.
        schema_version: 칸 구성을 바꿀 때 올리는 기록용 번호.
    """

    name: str
    key_columns: tuple[str, ...]
    source_columns: tuple[str, ...]
    date_column: str
    parsed_columns: Mapping[str, str] = field(default_factory=dict)
    schema_version: int = 1
    _all: tuple[str, ...] = field(init=False, repr=False, compare=False, default=())

    def __post_init__(self) -> None:
        if not self.key_columns:
            raise ValueError(f"{self.name}: key_columns가 비었습니다")
        if self.date_column not in self.key_columns:
            raise ValueError(f"{self.name}: date_column은 key_columns 안에 있어야 합니다")
        object.__setattr__(self, "parsed_columns", dict(self.parsed_columns))
        unknown_types = set(self.parsed_columns.values()) - set(_ARROW_TYPES)
        if unknown_types:
            raise ValueError(f"{self.name}: 모르는 형식입니다: {sorted(unknown_types)}")
        declared = [*self.key_columns, *self.source_columns, *self.parsed_columns]
        bad = set(declared) & (set(PROVENANCE_COLUMNS) | FORBIDDEN_COLUMNS)
        if bad:
            raise ValueError(f"{self.name}: 예약된 컬럼 이름을 씁니다: {sorted(bad)}")
        if set(self.parsed_columns) & (set(self.key_columns) | set(self.source_columns)):
            raise ValueError(f"{self.name}: parsed_columns가 다른 칸과 겹칩니다")
        names = list(dict.fromkeys([*self.key_columns, *self.source_columns, *self.parsed_columns]))
        if len({c.lower() for c in names}) != len(names):
            raise ValueError(f"{self.name}: 대소문자만 다른 칸이 있습니다 (DuckDB가 구분하지 않음)")
        # 키가 원천 칸에 없어도 된다(요청에서 온 bas_dd 등). 있으면 중복 없이 한 번만 둔다.
        ordered = list(self.key_columns)
        ordered += [c for c in self.source_columns if c not in self.key_columns]
        ordered += list(self.parsed_columns)
        object.__setattr__(self, "_all", tuple(ordered))

    @property
    def data_columns(self) -> tuple[str, ...]:
        """키 → 원천 → 숫자 순으로 중복 없이 나열한 데이터 칸."""
        return self._all

    def arrow_schema(self) -> pa.Schema:
        fields: list[pa.Field] = []
        for column in self._all:
            kind = _ARROW_TYPES[self.parsed_columns.get(column, "string")]
            fields.append(pa.field(column, kind))
        fields += [
            pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
            pa.field("fetched_at_basis", pa.string()),
            pa.field("obs_seq", pa.int64()),
            pa.field("obs_kind", pa.string()),
            pa.field("source_run", pa.string()),
            pa.field("run_id", pa.string()),
            pa.field("attempt", pa.int64()),
            pa.field("raw_sha256", pa.string()),
            pa.field("raw_path", pa.string()),
            pa.field("row_hash", pa.string()),
        ]
        return pa.schema(fields)


@dataclass
class ParsedResponse:
    """파서가 응답 하나에서 낸 결과. 열 구성은 ``spec``이 정한다.

    Args:
        spec: 이 응답이 들어갈 표.
        rows: 행 dict. 원천 칸은 원문 문자열(또는 ``None``), 숫자 칸은 숫자.
        scope: ``absent`` 판단 범위. 예: ``{"bas_dd": "20260814"}``. 키 칸이어야 한다.
        complete: 범위 전체를 완전히 받았나. 아니면 ``absent``를 만들지 않는다.
        absent_on_empty: ``rows``가 비어도 ``absent`` 근거로 쓰나. 기본은 아니다 —
            빈 응답(휴장·미발표)은 "행이 사라졌다"의 근거가 못 된다.
        day_kind, row_count, priced_rows, netasst_zero_rows, required_missing:
            요청 기록에 그대로 옮긴다. ``row_count``가 없으면 ``len(rows)``.
    """

    spec: TableSpec
    rows: Sequence[Mapping[str, Any]]
    scope: Mapping[str, str]
    complete: bool = True
    absent_on_empty: bool = False
    day_kind: str | None = None
    row_count: int | None = None
    priced_rows: int | None = None
    netasst_zero_rows: int | None = None
    required_missing: int | None = None


def row_hash(spec: TableSpec, row: Mapping[str, Any]) -> str:
    """원천 칸 원문 문자열을 고정 순서로 이은 sha256.

    받은 시각·실행 ID·숫자 변환값은 넣지 않는다. 넣으면 같은 값을 다시 받을
    때마다 새 관측이 생긴다. ``None``은 빈 문자열과 다르게 적는다.
    """
    parts: list[str] = []
    for column in spec.source_columns:
        value = row.get(column)
        if value is None:
            parts.append(_NULL)
        elif isinstance(value, str):
            parts.append("S" + value)
        else:
            raise TypeError(
                f"{spec.name}.{column}: 원천 칸은 원문 문자열이어야 합니다 ({type(value).__name__})"
            )
    return hashlib.sha256(_SEP.join(parts).encode("utf-8")).hexdigest()
