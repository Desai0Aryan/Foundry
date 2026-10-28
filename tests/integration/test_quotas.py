import asyncio
from datetime import UTC, datetime
from uuid import UUID

import pytest

from foundry import metrics
from foundry.quota import utc_day_start
from tests.conftest import ADMIN, make_client, make_user, set_limits, sleep_job

pytestmark = pytest.mark.integration


def job(i: int, **kw):
    return sleep_job(payload={"seconds": i}, **kw)


async def jobs_in_db(app) -> int:
    async with app.state.pool.acquire() as c:
        return await c.fetchval("SELECT count(*) FROM jobs")


async def usage(client, auth):
    r = await client.get("/v1/usage", headers=auth)
    assert r.status_code == 200, r.text
    return r.json()


async def limited_user(client, **limits):
    user, auth = await make_user(client)
    # a generous request rate so these tests exercise quotas, not the rate limiter
    await set_limits(client, user["id"], rate_limit_per_sec=1000, **limits)
    return user, auth


# ------------------------------------------------------------------------ daily quota
async def test_daily_quota_counts_down_then_rejects(client, app):
    _, auth = await limited_user(client, daily_job_quota=3, max_concurrent_jobs=100)
    remaining = []
    for i in range(3):
        r = await client.post("/v1/jobs", json=job(i), headers=auth)
        assert r.status_code == 201
        remaining.append(int(r.headers["X-Daily-Quota-Remaining"]))
        assert r.headers["X-Daily-Quota-Limit"] == "3"
    assert remaining == [2, 1, 0]

    before = metrics.LIMIT_REJECTIONS.labels("daily_quota")._value.get()
    r = await client.post("/v1/jobs", json=job(99), headers=auth)
    assert r.status_code == 429
    body = r.json()
    assert body["code"] == "quota_exceeded" and "3 jobs per UTC day" in body["detail"]
    assert int(r.headers["Retry-After"]) > 0 and r.headers["X-Daily-Quota-Remaining"] == "0"
    assert await jobs_in_db(app) == 3  # nothing was created
    assert metrics.LIMIT_REJECTIONS.labels("daily_quota")._value.get() - before == 1

    u = await usage(client, auth)
    assert (
        u["daily_quota"]["limit"] == 3
        and u["daily_quota"]["used"] == 3
        and u["daily_quota"]["remaining"] == 0
    )
    resets = datetime.fromisoformat(u["daily_quota"]["resets_at"])
    assert (
        resets > datetime.now(UTC)
        and resets.hour == 0
        and resets == utc_day_start() + (resets - utc_day_start())
    )


async def test_quota_is_per_user(client, app):
    _, a = await limited_user(client, daily_job_quota=1, max_concurrent_jobs=100)
    user_b, b = await make_user(client, "other@example.com")
    await set_limits(
        client, user_b["id"], rate_limit_per_sec=1000, daily_job_quota=1, max_concurrent_jobs=100
    )
    assert (await client.post("/v1/jobs", json=job(1), headers=a)).status_code == 201
    assert (await client.post("/v1/jobs", json=job(2), headers=a)).status_code == 429
    assert (await client.post("/v1/jobs", json=job(1), headers=b)).status_code == 201


async def test_invalid_requests_do_not_consume_quota(client, app):
    _, auth = await limited_user(client, daily_job_quota=2, max_concurrent_jobs=100)
    bad = await client.post(
        "/v1/jobs", json={"type": "demo.sleep", "payload": {"seconds": -1}}, headers=auth
    )
    assert bad.status_code == 422
    assert (await usage(client, auth))["daily_quota"]["used"] == 0


async def test_idempotent_replays_and_dedup_hits_are_free_even_over_quota(client, app):
    _, auth = await limited_user(client, daily_job_quota=2, max_concurrent_jobs=100)
    h = {**auth, "Idempotency-Key": "order-1"}
    first = await client.post("/v1/jobs", json=job(1), headers=h)
    assert first.status_code == 201
    dedup = await client.post("/v1/jobs", json=job(2, deduplicate=True), headers=auth)
    assert dedup.status_code == 201  # quota now fully used: 2 of 2

    # new work is refused...
    assert (await client.post("/v1/jobs", json=job(3), headers=auth)).status_code == 429
    # ...but a client safely retrying a request that already succeeded is NOT punished
    replay = await client.post("/v1/jobs", json=job(1), headers=h)
    assert replay.status_code == 200 and replay.headers["Idempotent-Replayed"] == "true"
    assert replay.json()["id"] == first.json()["id"]
    again = await client.post("/v1/jobs", json=job(2, deduplicate=True), headers=auth)
    assert again.status_code == 200 and again.headers["X-Deduplicated"] == "true"
    assert (await usage(client, auth))["daily_quota"]["used"] == 2  # replays cost nothing


async def test_refunds_keep_the_counter_exact_after_replays(client, app):
    _, auth = await limited_user(client, daily_job_quota=10, max_concurrent_jobs=100)
    h = {**auth, "Idempotency-Key": "same"}
    for _ in range(5):
        await client.post("/v1/jobs", json=job(1), headers=h)
    assert (await usage(client, auth))["daily_quota"]["used"] == 1
    async with app.state.pool.acquire() as c:
        assert await c.fetchval("SELECT count(*) FROM jobs") == 1


async def test_quota_is_exact_under_concurrent_submissions(client, app):
    _, auth = await limited_user(client, daily_job_quota=5, max_concurrent_jobs=1000)
    rs = await asyncio.gather(
        *(client.post("/v1/jobs", json=job(i), headers=auth) for i in range(30))
    )
    codes = sorted(r.status_code for r in rs)
    assert codes == [201] * 5 + [429] * 25
    assert await jobs_in_db(app) == 5
    assert (await usage(client, auth))["daily_quota"]["used"] == 5


async def test_counter_is_reseeded_from_postgres_after_redis_loses_its_data(client, app):
    _, auth = await limited_user(client, daily_job_quota=4, max_concurrent_jobs=100)
    for i in range(3):
        await client.post("/v1/jobs", json=job(i), headers=auth)
    await app.state.redis.flushdb()  # Redis restart / eviction: counters vanish
    r = await client.post("/v1/jobs", json=job(10), headers=auth)  # 4th of 4: allowed
    assert r.status_code == 201 and r.headers["X-Daily-Quota-Remaining"] == "0"
    assert (await client.post("/v1/jobs", json=job(11), headers=auth)).status_code == 429
    # a flush must not hand out a fresh quota
    await app.state.redis.flushdb()
    assert (await client.post("/v1/jobs", json=job(12), headers=auth)).status_code == 429


async def test_falls_back_to_postgres_when_redis_is_down(client, app):
    user, _ = await limited_user(client, daily_job_quota=2, max_concurrent_jobs=100)
    async with make_client(redis_url="redis://localhost:1/0") as c2:
        key = (await c2.post(f"/v1/admin/users/{user['id']}/api-keys", headers=ADMIN)).json()[
            "api_key"
        ]
        h = {"Authorization": f"Bearer {key}"}
        assert (await c2.post("/v1/jobs", json=job(1), headers=h)).status_code == 201
        assert (await c2.post("/v1/jobs", json=job(2), headers=h)).status_code == 201
        r = await c2.post("/v1/jobs", json=job(3), headers=h)
        assert r.status_code == 429 and r.json()["code"] == "quota_exceeded"


# ----------------------------------------------------------------- concurrency limit
async def test_concurrent_job_limit_is_exact_under_a_burst(client, app):
    _, auth = await limited_user(client, max_concurrent_jobs=3, daily_job_quota=1000)
    rs = await asyncio.gather(
        *(client.post("/v1/jobs", json=job(i), headers=auth) for i in range(15))
    )
    codes = sorted(r.status_code for r in rs)
    assert codes == [201] * 3 + [429] * 12
    rejected = next(r for r in rs if r.status_code == 429)
    assert rejected.json()["code"] == "concurrency_limit" and rejected.headers["Retry-After"] == "5"
    assert await jobs_in_db(app) == 3  # rejected inserts were rolled back
    u = await usage(client, auth)
    assert u["concurrency"] == {"limit": 3, "active": 3, "remaining": 0}
    assert u["daily_quota"]["used"] == 3  # rejected submissions gave their quota back


async def test_finishing_or_cancelling_a_job_frees_a_slot(client, app):
    _, auth = await limited_user(client, max_concurrent_jobs=2, daily_job_quota=1000)
    ids = [
        (await client.post("/v1/jobs", json=job(i), headers=auth)).json()["id"] for i in range(2)
    ]
    assert (await client.post("/v1/jobs", json=job(5), headers=auth)).status_code == 429
    await client.post(f"/v1/jobs/{ids[0]}/cancel", headers=auth)
    assert (await client.post("/v1/jobs", json=job(5), headers=auth)).status_code == 201


async def test_replays_work_when_at_the_concurrency_limit(client, app):
    _, auth = await limited_user(client, max_concurrent_jobs=1, daily_job_quota=1000)
    h = {**auth, "Idempotency-Key": "k"}
    assert (await client.post("/v1/jobs", json=job(1), headers=h)).status_code == 201
    assert (await client.post("/v1/jobs", json=job(2), headers=auth)).status_code == 429
    replay = await client.post("/v1/jobs", json=job(1), headers=h)
    assert replay.status_code == 200


async def test_concurrency_limits_do_not_block_other_users(client, app):
    _, a = await limited_user(client, max_concurrent_jobs=1, daily_job_quota=1000)
    ub, b = await make_user(client, "b@example.com")
    await set_limits(
        client, ub["id"], rate_limit_per_sec=1000, max_concurrent_jobs=1, daily_job_quota=1000
    )
    ra, rb = await asyncio.gather(
        client.post("/v1/jobs", json=job(1), headers=a),
        client.post("/v1/jobs", json=job(1), headers=b),
    )
    assert ra.status_code == rb.status_code == 201


# ------------------------------------------------------------------------- admin API
async def test_admin_can_change_tier_and_limits(client, app):
    user, auth = await make_user(client, tier="free")
    assert (await client.get("/v1/me", headers=auth)).json()["max_concurrent_jobs"] == 5
    r = await client.patch(
        f"/v1/admin/users/{user['id']}", json={"tier": "enterprise"}, headers=ADMIN
    )
    body = r.json()
    assert r.status_code == 200 and body["tier"] == "enterprise"
    assert (body["max_concurrent_jobs"], body["daily_job_quota"], body["rate_limit_per_sec"]) == (
        100,
        1_000_000,
        500.0,
    )
    custom = await set_limits(client, user["id"], daily_job_quota=7)
    assert custom["daily_job_quota"] == 7 and custom["tier"] == "enterprise"
    assert (await client.get("/v1/usage", headers=auth)).json()["daily_quota"]["limit"] == 7


async def test_admin_patch_validation_and_auth(client, app):
    user, _ = await make_user(client)
    url = f"/v1/admin/users/{user['id']}"
    assert (await client.patch(url, json={"daily_job_quota": 5})).status_code == 403
    assert (await client.patch(url, json={}, headers=ADMIN)).status_code == 400
    assert (await client.patch(url, json={"daily_job_quota": -1}, headers=ADMIN)).status_code == 422
    assert (await client.patch(url, json={"tier": "platinum"}, headers=ADMIN)).status_code == 422
    assert (await client.patch(url, json={"bogus": 1}, headers=ADMIN)).status_code == 422
    missing = await client.patch(
        f"/v1/admin/users/{UUID(int=1)}", json={"daily_job_quota": 5}, headers=ADMIN
    )
    assert missing.status_code == 404
