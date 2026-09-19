"""SEC 클라이언트 — 미국 계획 01 §1.1, 05 §6, 06 §1. 네트워크 없이 돈다."""

from __future__ import annotations

import zipfile

import pytest

from collector.us.sources import sec


class _Resp:
    def __init__(self, status=200, body=b"", headers=None):
        self.status_code = status
        self._body = body
        self.headers = headers or {}

    def iter_content(self, chunk_size=1):
        yield self._body


class _Session:
    def __init__(self, resp):
        self.resp = resp
        self.calls = []

    def get(self, url, **kw):
        self.calls.append(url)
        return self.resp


# --- UA --------------------------------------------------------------------


def test_user_agent_is_required():
    with pytest.raises(sec.SecAccessError, match="SEC_USER_AGENT"):
        sec.user_agent_from_env({})


def test_user_agent_comes_from_env():
    assert sec.user_agent_from_env({"SEC_USER_AGENT": "x/1 (a@b.c)"}) == "x/1 (a@b.c)"


# --- 분기·URL ---------------------------------------------------------------


def test_quarters_spans_the_test_window():
    qs = sec.quarters((2018, 3), (2026, 2))
    assert len(qs) == 32  # 계획이 말하는 "32분기"
    assert qs[0] == (2018, 3) and qs[-1] == (2026, 2)
    assert (2018, 4) in qs and (2019, 1) in qs


def test_url_shapes():
    assert sec.financial_statements_url(2018, 4).endswith("/2018q4.zip")
    assert sec.insider_url(2018, 4).endswith("/2018q4_form345.zip")
    # MIDAS만 밑줄 형식이다 — individual_security_2018_q4.zip
    assert sec.midas_url(2018, 4).endswith("/individual_security_2018_q4.zip")


def test_midas_2019q4_uses_the_exception_path():
    """SEC 목록이 이 한 분기만 Drupal 내부 경로로 건다. 규칙대로 만들면 404다."""
    assert (2019, 4) in sec.MIDAS_URL_EXCEPTIONS
    assert "/files/node/add/data_distribution/" in sec.midas_url(2019, 4)
    # 이웃 분기는 규칙 그대로여야 한다
    for q in (3,):
        assert "metrics-individual-security" in sec.midas_url(2019, q)
    assert "metrics-individual-security" in sec.midas_url(2020, 1)


# --- 403은 재시도하지 않는다 -------------------------------------------------


def test_403_raises_and_says_to_look_at_the_ua():
    client = sec.SecClient(user_agent="x/1 (a@b.c)", session=_Session(_Resp(403)))
    with pytest.raises(sec.SecAccessError, match="UA에 연락처"):
        client.get("https://www.sec.gov/anything")


def test_non_200_raises():
    client = sec.SecClient(user_agent="x/1 (a@b.c)", session=_Session(_Resp(404)))
    with pytest.raises(sec.SecAccessError, match="404"):
        client.get("https://www.sec.gov/anything")


def test_interval_lives_inside_the_client():
    """부르는 쪽이 잊어도 지켜져야 한다 (02 §2.1)."""
    client = sec.SecClient(user_agent="x/1 (a@b.c)", session=_Session(_Resp(200)))
    assert client.interval_seconds == sec.DEFAULT_INTERVAL_SECONDS == 5.0


# --- 200이 성공이 아니다 -----------------------------------------------------


def test_html_saved_as_zip_is_caught(tmp_path):
    """403 HTML을 .zip으로 저장한 것 — 크기만 보면 못 잡는다 (06 §1)."""
    bad = tmp_path / "2018q4.zip"
    bad.write_bytes(b"<html><body>SEC.gov | Request Rate Threshold Exceeded</body></html>")
    with pytest.raises(sec.SecAccessError, match="ZIP 매직이 아니다"):
        sec.assert_is_zip(bad)


def test_empty_zip_is_caught(tmp_path):
    p = tmp_path / "empty.zip"
    with zipfile.ZipFile(p, "w"):
        pass
    with pytest.raises(sec.SecAccessError, match="entry가 없다"):
        sec.assert_is_zip(p)


def test_truncated_zip_is_caught(tmp_path):
    p = tmp_path / "cut.zip"
    p.write_bytes(b"PK\x03\x04" + b"\x00" * 20)
    with pytest.raises(sec.SecAccessError, match="열리지 않는다"):
        sec.assert_is_zip(p)


def test_real_zip_returns_entry_names(tmp_path):
    p = tmp_path / "ok.zip"
    with zipfile.ZipFile(p, "w") as zf:
        zf.writestr("sub.txt", "adsh\tcik\tsic\n")
    assert sec.assert_is_zip(p) == ["sub.txt"]


def test_download_leaves_no_partial_file_behind(tmp_path):
    dest = tmp_path / "x" / "f.zip"
    client = sec.SecClient(user_agent="x/1 (a@b.c)", session=_Session(_Resp(200, b"PK\x03\x04")))
    client.download("https://www.sec.gov/f.zip", dest)
    assert dest.read_bytes() == b"PK\x03\x04"
    assert not list(dest.parent.glob("*.part"))


# --- 분기 ZIP 내려받기 -------------------------------------------------------


def test_quarterly_path_layout(tmp_path):
    from collector.lake import DataRoot

    p = sec.quarterly_path(DataRoot(tmp_path), "financial", 2018, 4)
    assert p.parts[-4:] == ("sec", "quarterly", "financial", "2018q4.zip")


def test_quarterly_path_rejects_unknown_kind(tmp_path):
    from collector.lake import DataRoot

    with pytest.raises(ValueError, match="모르는 갈래"):
        sec.quarterly_path(DataRoot(tmp_path), "nope", 2018, 4)


def _zip_bytes():
    import io as _io

    buf = _io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("sub.txt", "adsh\tcik\tsic\n")
    return buf.getvalue()


def test_download_quarterly_skips_a_good_file(tmp_path):
    from collector.lake import DataRoot

    root = DataRoot(tmp_path)
    dest = sec.quarterly_path(root, "midas", 2020, 1)
    dest.parent.mkdir(parents=True)
    dest.write_bytes(_zip_bytes())
    session = _Session(_Resp(200, b"should-not-be-used"))
    r = sec.download_quarterly(
        sec.SecClient("x/1 (a@b.c)", session=session), root, "midas", 2020, 1
    )
    assert r["skipped"] is True
    assert session.calls == []  # 요청을 아예 안 보낸다


def test_download_quarterly_refetches_a_broken_file(tmp_path):
    """403 HTML이 .zip으로 남아 있으면 지우고 다시 받는다 — 이어받기의 근거다."""
    from collector.lake import DataRoot

    root = DataRoot(tmp_path)
    dest = sec.quarterly_path(root, "midas", 2020, 1)
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"<html>Request Rate Threshold Exceeded</html>")
    session = _Session(_Resp(200, _zip_bytes()))
    r = sec.download_quarterly(
        sec.SecClient("x/1 (a@b.c)", session=session), root, "midas", 2020, 1
    )
    assert r["skipped"] is False
    assert len(session.calls) == 1
    assert sec.assert_is_zip(dest) == ["sub.txt"]
