import pytest

from tests.conftest import ADMIN, make_user

pytestmark = pytest.mark.integration


async def test_liveness_and_readiness(client):
    assert (await client.get("/healthz")).json() == {"status": "ok"}
    ready = await client.get("/readyz")
    assert ready.status_code == 200
    assert ready.json()["database"] == "up"


async def test_job_type_catalog_exposes_schemas(client):
    r = await client.get("/v1/job-types")
    assert r.status_code == 200
    types = {t["type"]: t for t in r.json()}
    assert {"document.convert", "video.transcode", "ml.inference", "demo.sleep"} <= set(types)
    assert "source_url" in types["video.transcode"]["payload_schema"]["properties"]


async def test_admin_requires_token(client):
    body = {"email": "x@example.com", "name": "X"}
    assert (await client.post("/v1/admin/users", json=body)).status_code == 403
    bad = await client.post("/v1/admin/users", json=body, headers={"X-Admin-Token": "nope"})
    assert bad.status_code == 403
    assert bad.headers["content-type"].startswith("application/problem+json")


async def test_create_user_returns_key_once_and_tier_limits(client):
    r = await client.post(
        "/v1/admin/users",
        json={"email": "Pro@Example.com", "name": "P", "tier": "pro"},
        headers=ADMIN,
    )
    assert r.status_code == 201
    body = r.json()
    assert body["api_key"].startswith("fnd_")
    assert body["user"]["email"] == "pro@example.com"  # normalized
    assert body["user"]["max_concurrent_jobs"] == 25
    dup = await client.post(
        "/v1/admin/users", json={"email": "pro@example.com", "name": "P"}, headers=ADMIN
    )
    assert dup.status_code == 409


async def test_authentication(client):
    user, headers = await make_user(client)
    assert (await client.get("/v1/me")).status_code == 401
    assert (
        await client.get("/v1/me", headers={"Authorization": "Bearer fnd_deadbeef_x"})
    ).status_code == 401
    assert (
        await client.get("/v1/me", headers={"Authorization": "Bearer garbage"})
    ).status_code == 401
    ok = await client.get("/v1/me", headers=headers)
    assert ok.status_code == 200 and ok.json()["id"] == user["id"]
    # X-API-Key header works too
    token = headers["Authorization"].split()[1]
    assert (await client.get("/v1/me", headers={"X-API-Key": token})).status_code == 200


async def test_tampered_secret_is_rejected(client):
    _, headers = await make_user(client)
    token = headers["Authorization"].split()[1]
    tampered = token[:-1] + ("A" if token[-1] != "A" else "B")
    r = await client.get("/v1/me", headers={"Authorization": f"Bearer {tampered}"})
    assert r.status_code == 401
    assert r.headers["www-authenticate"] == "Bearer"


async def test_revoked_key_is_rejected(client, app):
    _, headers = await make_user(client)
    async with app.state.pool.acquire() as conn:
        await conn.execute("UPDATE api_keys SET revoked_at = now()")
    assert (await client.get("/v1/me", headers=headers)).status_code == 401


async def test_key_rotation_issues_a_second_working_key(client):
    user, headers = await make_user(client)
    r = await client.post(f"/v1/admin/users/{user['id']}/api-keys", headers=ADMIN)
    assert r.status_code == 201
    new = {"Authorization": f"Bearer {r.json()['api_key']}"}
    assert (await client.get("/v1/me", headers=new)).status_code == 200
    assert (await client.get("/v1/me", headers=headers)).status_code == 200


async def test_request_id_is_echoed_and_errors_are_problem_json(client):
    r = await client.get("/v1/me", headers={"X-Request-ID": "trace-123"})
    assert r.headers["X-Request-ID"] == "trace-123"
    body = r.json()
    assert body["code"] == "unauthorized" and body["request_id"] == "trace-123"
