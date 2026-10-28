"""API key generation and verification.

Token format:  fnd_<prefix>_<secret>
  * prefix  - 8 hex chars, stored in clear, used for O(1) lookup
  * secret  - 256 bits of entropy, stored only as HMAC-SHA256(pepper, secret)

Keys are high-entropy random values, so a keyed fast hash is appropriate (no need for
bcrypt/argon2, which exist to slow down guessing of *low*-entropy passwords).
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
from dataclasses import dataclass

TOKEN_NAMESPACE = "fnd"  # noqa: S105


@dataclass(frozen=True)
class GeneratedKey:
    token: str
    prefix: str
    secret_hash: str


def hash_secret(secret: str, pepper: str) -> str:
    return hmac.new(pepper.encode(), secret.encode(), hashlib.sha256).hexdigest()


def generate_api_key(pepper: str) -> GeneratedKey:
    prefix = secrets.token_hex(4)
    secret = secrets.token_urlsafe(32)
    return GeneratedKey(
        token=f"{TOKEN_NAMESPACE}_{prefix}_{secret}",
        prefix=prefix,
        secret_hash=hash_secret(secret, pepper),
    )


def parse_api_key(token: str) -> tuple[str, str] | None:
    parts = token.strip().split("_", 2)
    if len(parts) != 3 or parts[0] != TOKEN_NAMESPACE:
        return None
    _, prefix, secret = parts
    if len(prefix) != 8 or not secret:
        return None
    return prefix, secret


def verify_secret(secret: str, pepper: str, expected_hash: str) -> bool:
    return hmac.compare_digest(hash_secret(secret, pepper), expected_hash)


def constant_time_equals(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode(), b.encode())
