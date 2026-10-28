"""Registry of job types: payload schemas plus per-type execution defaults.

Adding a new workload = one pydantic model + one registry entry. The API validates and
*normalizes* payloads (defaults filled in), so semantically-equal requests hash equally.
"""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass
from typing import Annotated, Any, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, HttpUrl

JobType = Literal["document.convert", "video.transcode", "ml.inference", "demo.sleep"]

_BLOCKED_SUFFIXES = (".internal", ".local", ".localhost")
_BLOCKED_HOSTS = {"localhost", "metadata.google.internal"}


def _require_public_url(url: HttpUrl) -> HttpUrl:
    """First-line SSRF guard. Workers MUST re-validate after DNS resolution (rebinding)."""
    host = (url.host or "").strip("[]").lower()
    if host in _BLOCKED_HOSTS or host.endswith(_BLOCKED_SUFFIXES):
        raise ValueError("source_url must point to a public host")
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return url  # a DNS name; checked again at fetch time by the worker
    if (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast
        or ip.is_unspecified
    ):
        raise ValueError("source_url must not target private or reserved addresses")
    return url


PublicUrl = Annotated[HttpUrl, AfterValidator(_require_public_url)]


class _Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DocumentConvert(_Payload):
    source_url: PublicUrl
    output_format: Literal["pdf", "docx", "html", "md"]
    ocr: bool = False
    page_range: str | None = Field(default=None, pattern=r"^\d+(-\d+)?(,\d+(-\d+)?)*$")


class VideoTranscode(_Payload):
    source_url: PublicUrl
    preset: Literal["480p", "720p", "1080p", "4k"] = "720p"
    codec: Literal["h264", "h265", "av1"] = "h264"
    audio_bitrate_kbps: int = Field(default=128, ge=32, le=512)


class MlInference(_Payload):
    model: str = Field(pattern=r"^[a-z0-9][a-z0-9._/-]{1,127}$")
    inputs: list[Any] = Field(min_length=1, max_length=1000)
    parameters: dict[str, Any] = Field(default_factory=dict)


class DemoSleep(_Payload):
    """Synthetic workload used by load tests and crash-recovery demos."""

    seconds: float = Field(default=1.0, ge=0, le=600)
    fail_probability: float = Field(default=0.0, ge=0, le=1)
    side_effect_key: str | None = Field(default=None, max_length=128)


@dataclass(frozen=True)
class JobTypeSpec:
    name: str
    description: str
    payload_model: type[BaseModel]
    default_timeout_seconds: int
    default_max_attempts: int


REGISTRY: dict[str, JobTypeSpec] = {
    s.name: s
    for s in (
        JobTypeSpec(
            "document.convert",
            "Convert a document between formats (PDF, DOCX, HTML, Markdown), optional OCR.",
            DocumentConvert,
            default_timeout_seconds=300,
            default_max_attempts=4,
        ),
        JobTypeSpec(
            "video.transcode",
            "Transcode a video to a target resolution and codec.",
            VideoTranscode,
            default_timeout_seconds=1800,
            default_max_attempts=3,
        ),
        JobTypeSpec(
            "ml.inference",
            "Run batched ML model inference.",
            MlInference,
            default_timeout_seconds=120,
            default_max_attempts=5,
        ),
        JobTypeSpec(
            "demo.sleep",
            "Synthetic job for load tests and chaos demos: sleeps, may fail, records an effect.",
            DemoSleep,
            default_timeout_seconds=60,
            default_max_attempts=5,
        ),
    )
}
