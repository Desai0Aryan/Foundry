import asyncio
import uuid

import pytest
import redis.asyncio as aioredis

from foundry import metrics
from foundry.ratelimit import RateLimiter
from tests.conftest import ADMIN, REDIS_URL, make_client, make_settings, make_user, set_limits

pytestmark = pytest.mark.integration


@pytest.fixture
async def rds(app):
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    try:
        await r.ping()
    except Exception as exc:
        pytest.skip(f"Redis not reachable at {REDIS_URL}: {exc}")
    yield r
    await r.aclose()


def counter(result: str) -> float:
    return metrics.RATE_LIMIT_DECISIONS.labels(result)._value.get()


# --------------------------------------------------------------- the token bucket itself
async def test_burst_then_throttle_then_refill(rds):
    limiter = RateLimiter(rds, make_settings())  # burst_seconds=2 -> capacity = rate * 2
    uid = str(uuid.uuid4())
    decisions = [await limiter.check(uid, rate=5) for _ in range(12)]
    assert [d.allowed for d in decisions] == [True] * 10 + [False] * 2
    assert decisions[0].limit == 10 and decisions[0].remaining == 9
    denied = decisions[-1]
    assert denied.retry_after >= 1 and denied.headers()["Retry-After"] == str(denied.retry_after)
    await asyncio.sleep(0.6)  # 5 tokens/s * 0.6s = 3 tokens back
    again = [await limiter.check(uid, rate=5) for _ in range(4)]
    assert [d.allowed for d in again] == [True, True, True, False]


async def test_bucket_is_atomic_under_concurrency(rds):
    # slow refill (0.5 tokens/s) so the count cannot drift while the calls are in flight
    limiter = RateLimiter(rds, make_settings(rate_limit_burst_seconds=20))
    uid = str(uuid.uuid4())
    results = await asyncio.gather(*(limiter.check(uid, rate=0.5) for _ in range(60)))
    assert sum(d.allowed for d in results) == 10  # exactly the burst, no more, no less


async def test_replicas_share_one_bucket(rds):
    """Two API replicas = two limiter objects (even two connections). One shared budget."""
    other = aioredis.from_url(REDIS_URL, decode_responses=True)
    s = make_settings(rate_limit_burst_seconds=20)
    a, b = RateLimiter(rds, s), RateLimiter(other, s)
    uid = str(uuid.uuid4())
    results = await asyncio.gather(*(lim.check(uid, rate=0.5) for lim in (a, b) for _ in range(20)))
    await other.aclose()
    assert sum(d.allowed for d in results) == 10


async def test_users_have_independent_buckets(rds):
    limiter = RateLimiter(rds, make_settings())
    u1, u2 = str(uuid.uuid4()), str(uuid.uuid4())
    for _ in range(10):
        await limiter.check(u1, rate=5)
    assert not (await limiter.check(u1, rate=5)).allowed
    assert (await limiter.check(u2, rate=5)).allowed


async def test_tiny_rates_still_allow_one_request(rds):
    limiter = RateLimiter(rds, make_settings())
    d = await limiter.check(str(uuid.uuid4()), rate=0.1)  # capacity floors at 1
    assert d.allowed and d.limit == 1


async def test_bucket_keys_expire_so_idle_users_cost_nothing(rds):
    limiter = RateLimiter(rds, make_settings())
    uid = str(uuid.uuid4())
    await limiter.check(uid, rate=5)
    ttl = await rds.pttl(f"foundry:rl:{uid}")
    assert 0 < ttl <= 3000


# ------------------------------------------------------------------------- over HTTP
async def test_http_429_with_standard_headers_and_recovery(client, app):
    user, auth = await make_user(client)
    await set_limits(client, user["id"], rate_limit_per_sec=2)  # capacity 4
    statuses, remaining = [], []
    for _ in range(7):
        r = await client.get("/v1/me", headers=auth)
        statuses.append(r.status_code)
        if r.status_code == 200:
            remaining.append(int(r.headers["RateLimit-Remaining"]))
    assert statuses[:4] == [200] * 4 and set(statuses[4:]) == {429}
    assert remaining == [3, 2, 1, 0]

    limited = await client.get("/v1/me", headers=auth)
    body = limited.json()
    assert body["code"] == "rate_limited" and limited.headers["content-type"].startswith(
        "application/problem+json"
    )
    assert int(limited.headers["Retry-After"]) >= 1
    assert (
        limited.headers["RateLimit-Limit"] == "4" and limited.headers["RateLimit-Remaining"] == "0"
    )
    assert limited.headers["X-Request-ID"] == body["request_id"]

    await asyncio.sleep(1.1)  # 2 tokens/s
    assert (await client.get("/v1/me", headers=auth)).status_code == 200


async def test_one_noisy_user_does_not_starve_another(client, app):
    noisy, noisy_auth = await make_user(client, "noisy@example.com")
    _, calm_auth = await make_user(client, "calm@example.com")
    await set_limits(client, noisy["id"], rate_limit_per_sec=1)
    for _ in range(10):
        await client.get("/v1/me", headers=noisy_auth)
    assert (await client.get("/v1/me", headers=noisy_auth)).status_code == 429
    assert (await client.get("/v1/me", headers=calm_auth)).status_code == 200


async def test_rate_limit_applies_to_every_authenticated_endpoint(client, app):
    user, auth = await make_user(client)
    await set_limits(client, user["id"], rate_limit_per_sec=1)  # capacity 2
    await client.get("/v1/me", headers=auth)
    await client.get("/v1/me", headers=auth)
    for method, path in [("GET", "/v1/jobs"), ("GET", "/v1/usage"), ("POST", "/v1/jobs")]:
        r = await client.request(method, path, headers=auth, json={} if method == "POST" else None)
        assert r.status_code == 429, path
    assert (await client.get("/healthz")).status_code == 200  # probes are never limited
    assert (await client.get("/v1/job-types")).status_code == 200


async def test_rate_limit_decisions_are_counted(client, app):
    user, auth = await make_user(client)
    await set_limits(client, user["id"], rate_limit_per_sec=1)
    allowed0, limited0 = counter("allowed"), counter("limited")
    for _ in range(5):
        await client.get("/v1/me", headers=auth)
    assert counter("allowed") - allowed0 == 2 and counter("limited") - limited0 == 3


async def test_changing_the_rate_takes_effect_immediately(client, app):
    user, auth = await make_user(client)
    await set_limits(client, user["id"], rate_limit_per_sec=1)
    for _ in range(3):
        await client.get("/v1/me", headers=auth)
    assert (await client.get("/v1/me", headers=auth)).status_code == 429
    await set_limits(client, user["id"], rate_limit_per_sec=1000)
    await asyncio.sleep(0.05)  # bucket refills at the new rate
    assert (await client.get("/v1/me", headers=auth)).status_code == 200


# ----------------------------------------------------------------------- failure modes
async def test_fails_open_when_redis_is_down(client, app):
    user, _ = await make_user(client)
    async with make_client(redis_url="redis://localhost:1/0") as c2:
        auth = {
            "Authorization": "Bearer "
            + (await c2.post(f"/v1/admin/users/{user['id']}/api-keys", headers=ADMIN)).json()[
                "api_key"
            ]
        }
        before = counter("error_open")
        for _ in range(3):
            assert (await c2.get("/v1/me", headers=auth)).status_code == 200
        assert counter("error_open") - before == 3


async def test_can_fail_closed_when_configured(client, app):
    user, _ = await make_user(client)
    async with make_client(redis_url="redis://localhost:1/0", rate_limit_fail_open=False) as c2:
        key = (await c2.post(f"/v1/admin/users/{user['id']}/api-keys", headers=ADMIN)).json()[
            "api_key"
        ]
        r = await c2.get("/v1/me", headers={"Authorization": f"Bearer {key}"})
        assert r.status_code == 429


async def test_can_be_disabled(client, app):
    user, _ = await make_user(client)
    await set_limits(client, user["id"], rate_limit_per_sec=1)
    async with make_client(rate_limit_enabled=False) as c2:
        key = (await c2.post(f"/v1/admin/users/{user['id']}/api-keys", headers=ADMIN)).json()[
            "api_key"
        ]
        codes = {
            (await c2.get("/v1/me", headers={"Authorization": f"Bearer {key}"})).status_code
            for _ in range(10)
        }
        assert codes == {200}


# ----------------------------------------------------------- credential-guessing guard
async def test_repeated_bad_credentials_are_throttled_but_valid_keys_still_work(client, app):
    _, good = await make_user(client)
    async with make_client(auth_fail_limit_per_minute=5) as c2:
        codes = []
        for i in range(9):
            r = await c2.get("/v1/me", headers={"Authorization": f"Bearer fnd_abcdef12_guess{i}"})
            codes.append(r.status_code)
        assert codes == [401] * 5 + [429] * 4
        last = await c2.get("/v1/me", headers={"Authorization": "Bearer fnd_abcdef12_guessX"})
        assert (
            last.json()["code"] == "too_many_auth_failures" and last.headers["Retry-After"] == "60"
        )
        # the legitimate user behind the same IP is NOT locked out
        assert (await c2.get("/v1/me", headers=good)).status_code == 200
