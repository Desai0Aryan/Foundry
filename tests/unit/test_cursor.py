from datetime import UTC, datetime
from uuid import uuid4

import pytest

from foundry.repository import decode_cursor, encode_cursor


def test_cursor_roundtrip_preserves_microseconds():
    ts = datetime(2026, 10, 26, 12, 30, 45, 123456, tzinfo=UTC)
    ident = uuid4()
    assert decode_cursor(encode_cursor(ts, ident)) == (ts, ident)


@pytest.mark.parametrize("bad", ["", "!!!", "bm90LWEtY3Vyc29y", "Zm9vfGJhcg"])
def test_bad_cursors_raise(bad):
    with pytest.raises(ValueError):
        decode_cursor(bad)
