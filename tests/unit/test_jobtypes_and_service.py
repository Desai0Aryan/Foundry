import pytest

from foundry.errors import ApiError
from foundry.schemas import JobCreate
from foundry.service import prepare_job

LIMIT = 256 * 1024


def prep(**kw):
    return prepare_job(JobCreate(**kw), LIMIT)


def test_defaults_are_normalized_so_equivalent_requests_hash_equally():
    a = prep(type="demo.sleep", payload={"seconds": 1})
    b = prep(type="demo.sleep", payload={"seconds": 1.0, "fail_probability": 0})
    assert a.payload == b.payload
    assert a.payload_hash == b.payload_hash
    assert a.request_hash == b.request_hash


def test_key_order_does_not_matter():
    a = prep(
        type="ml.inference",
        payload={"model": "bert-base", "inputs": [1], "parameters": {"a": 1, "b": 2}},
    )
    b = prep(
        type="ml.inference",
        payload={"parameters": {"b": 2, "a": 1}, "inputs": [1], "model": "bert-base"},
    )
    assert a.payload_hash == b.payload_hash


def test_request_hash_covers_options_but_payload_hash_does_not():
    a = prep(type="demo.sleep", payload={"seconds": 1}, priority=1)
    b = prep(type="demo.sleep", payload={"seconds": 1}, priority=9)
    assert a.payload_hash == b.payload_hash
    assert a.request_hash != b.request_hash


def test_dedup_key_only_when_requested():
    assert prep(type="demo.sleep", payload={}).dedup_key is None
    assert prep(type="demo.sleep", payload={}, deduplicate=True).dedup_key is not None


def test_type_defaults_applied():
    video = prep(type="video.transcode", payload={"source_url": "https://example.com/v.mp4"})
    assert video.timeout_seconds == 1800
    assert video.max_attempts == 3
    assert prep(type="demo.sleep", payload={}, max_attempts=2, timeout_seconds=9).max_attempts == 2


@pytest.mark.parametrize(
    "url",
    [
        "http://localhost/x",
        "http://127.0.0.1/x",
        "http://10.0.0.5/x",
        "http://169.254.169.254/latest/meta-data",
        "http://[::1]/x",
        "http://metadata.google.internal/x",
        "http://printer.local/x",
    ],
)
def test_ssrf_guard_blocks_internal_targets(url):
    with pytest.raises(ApiError) as exc:
        prep(type="document.convert", payload={"source_url": url, "output_format": "pdf"})
    assert exc.value.status == 422
    assert exc.value.errors[0]["loc"][:2] == ["payload", "source_url"]


def test_public_urls_pass():
    ok = prep(
        type="document.convert",
        payload={"source_url": "https://example.com/a.docx", "output_format": "pdf"},
    )
    assert ok.payload["source_url"] == "https://example.com/a.docx"


def test_unknown_payload_fields_rejected():
    with pytest.raises(ApiError) as exc:
        prep(type="demo.sleep", payload={"seconds": 1, "bogus": True})
    assert exc.value.code == "invalid_payload"


def test_payload_size_limit():
    with pytest.raises(ApiError) as exc:
        prepare_job(
            JobCreate(type="ml.inference", payload={"model": "bert", "inputs": ["x" * 500] * 20}),
            1000,
        )
    assert exc.value.status == 413
