from __future__ import annotations

from typing import Any


class ApiError(Exception):
    """An error that renders as an RFC 9457 problem+json response."""

    def __init__(
        self,
        status: int,
        code: str,
        title: str,
        detail: str | None = None,
        *,
        errors: list[dict[str, Any]] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(f"{status} {code}: {title}")
        self.status = status
        self.code = code
        self.title = title
        self.detail = detail
        self.errors = errors
        self.headers = headers or {}


def unauthorized(detail: str = "Missing or invalid API key.") -> ApiError:
    return ApiError(
        401, "unauthorized", "Unauthorized", detail, headers={"WWW-Authenticate": "Bearer"}
    )


def not_found(what: str = "Resource") -> ApiError:
    return ApiError(404, "not_found", "Not found", f"{what} not found.")
