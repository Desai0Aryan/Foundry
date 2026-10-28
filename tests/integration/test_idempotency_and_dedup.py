import asyncio

import pytest

from tests.conftest import make_user, sleep_job

pytestmark = pytest.mark.integration


async def count(app, table="jobs"):
    async with app.state.pool.acquire() as conn:
        return await conn.fetchval(f"SELECT count(*) FROM {table}")  # noqa: S608


# ------------------------------------------------------------------ idempotency keys
async def test_same_key_same_request_replays_original(client, auth, app):
    h = {**auth, "Idempotency-Key": "order-42"}
    first = await client.post("/v1/jobs", json=sleep_job(), headers=h)
    second = await client.post("/v1/jobs", json=sleep_job(), headers=h)
    assert first.status_code == 201 and "Idempotent-Replayed" not in first.headers
    assert second.status_code == 200 and second.headers["Idempotent-Replayed"] == "true"
    assert first.json()["id"] == second.json()["id"]
    assert await count(app) == 1 and await count(app, "outbox") == 1


async def test_same_key_different_request_is_rejected(client, auth, app):
    h = {**auth, "Idempotency-Key": "k1"}
    await client.post("/v1/jobs", json=sleep_job(payload={"seconds": 1}), headers=h)
    r = await client.post("/v1/jobs", json=sleep_job(payload={"seconds": 2}), headers=h)
    assert r.status_code == 422 and r.json()["code"] == "idempotency_key_reuse"
    assert await count(app) == 1


async def test_semantically_equal_requests_replay_despite_formatting_differences(client, auth):
    h = {**auth, "Idempotency-Key": "k-norm"}
    a = await client.post("/v1/jobs", json=sleep_job(payload={"seconds": 1}), headers=h)
    b = await client.post(
        "/v1/jobs", json=sleep_job(payload={"fail_probability": 0, "seconds": 1.0}), headers=h
    )
    assert b.status_code == 200 and a.json()["id"] == b.json()["id"]


async def test_keys_are_scoped_per_user(client, auth):
    _, other = await make_user(client, email="bob@example.com")
    a = await client.post(
        "/v1/jobs", json=sleep_job(), headers={**auth, "Idempotency-Key": "shared"}
    )
    b = await client.post(
        "/v1/jobs", json=sleep_job(), headers={**other, "Idempotency-Key": "shared"}
    )
    assert a.status_code == b.status_code == 201
    assert a.json()["id"] != b.json()["id"]


@pytest.mark.parametrize("key", ["has space", "x" * 256, "naïve"])
async def test_invalid_idempotency_keys_rejected(client, auth, key):
    try:
        r = await client.post(
            "/v1/jobs", json=sleep_job(), headers={**auth, "Idempotency-Key": key}
        )
    except UnicodeEncodeError:
        return  # the HTTP client itself refuses non-ASCII header values
    assert r.status_code == 400 and r.json()["code"] == "invalid_idempotency_key"


async def test_concurrent_requests_with_one_key_create_exactly_one_job(client, auth, app):
    """25 simultaneous retries of the same request: the database picks one winner."""
    h = {**auth, "Idempotency-Key": "stampede"}
    responses = await asyncio.gather(
        *(client.post("/v1/jobs", json=sleep_job(), headers=h) for _ in range(25))
    )
    statuses = sorted(r.status_code for r in responses)
    assert statuses == [200] * 24 + [201]
    assert len({r.json()["id"] for r in responses}) == 1
    assert await count(app) == 1
    assert await count(app, "outbox") == 1
    assert await count(app, "job_events") == 1


# ------------------------------------------------------------------------ dedup
async def test_dedup_collapses_identical_active_jobs(client, auth, app):
    body = sleep_job(deduplicate=True, payload={"seconds": 3})
    first = await client.post("/v1/jobs", json=body, headers=auth)
    second = await client.post("/v1/jobs", json=body, headers=auth)
    assert first.status_code == 201
    assert second.status_code == 200 and second.headers["X-Deduplicated"] == "true"
    assert first.json()["id"] == second.json()["id"]
    assert await count(app) == 1


async def test_dedup_ignores_key_order_and_defaults(client, auth):
    a = await client.post(
        "/v1/jobs", json=sleep_job(deduplicate=True, payload={"seconds": 4}), headers=auth
    )
    b = await client.post(
        "/v1/jobs",
        json=sleep_job(deduplicate=True, payload={"fail_probability": 0.0, "seconds": 4}),
        headers=auth,
    )
    assert b.status_code == 200 and a.json()["id"] == b.json()["id"]


async def test_dedup_is_opt_in_and_per_user(client, auth, app):
    plain = sleep_job(payload={"seconds": 5})
    await client.post("/v1/jobs", json=plain, headers=auth)
    await client.post("/v1/jobs", json=plain, headers=auth)
    assert await count(app) == 2  # no dedup requested -> two jobs

    _, other = await make_user(client, email="eve@example.com")
    d = sleep_job(deduplicate=True, payload={"seconds": 6})
    a = await client.post("/v1/jobs", json=d, headers=auth)
    b = await client.post("/v1/jobs", json=d, headers=other)
    assert a.status_code == b.status_code == 201 and a.json()["id"] != b.json()["id"]


async def test_dedup_slot_frees_when_job_finishes(client, auth):
    body = sleep_job(deduplicate=True, payload={"seconds": 7})
    first = (await client.post("/v1/jobs", json=body, headers=auth)).json()
    await client.post(f"/v1/jobs/{first['id']}/cancel", headers=auth)  # terminal state
    again = await client.post("/v1/jobs", json=body, headers=auth)
    assert again.status_code == 201 and again.json()["id"] != first["id"]


async def test_concurrent_dedup_creates_exactly_one_job(client, auth, app):
    body = sleep_job(deduplicate=True, payload={"seconds": 8})
    responses = await asyncio.gather(
        *(client.post("/v1/jobs", json=body, headers=auth) for _ in range(20))
    )
    assert sorted(r.status_code for r in responses) == [200] * 19 + [201]
    assert len({r.json()["id"] for r in responses}) == 1
    assert await count(app) == 1 and await count(app, "outbox") == 1
