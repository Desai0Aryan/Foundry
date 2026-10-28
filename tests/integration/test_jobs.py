import pytest

from tests.conftest import make_user, sleep_job

pytestmark = pytest.mark.integration


async def test_submit_creates_job_event_and_outbox_atomically(client, auth, app):
    r = await client.post("/v1/jobs", json=sleep_job(priority=7), headers=auth)
    assert r.status_code == 201
    job = r.json()
    assert job["status"] == "queued" and job["priority"] == 7 and job["attempts"] == 0
    assert job["max_attempts"] == 5 and job["timeout_seconds"] == 60
    assert r.headers["Location"] == f"/v1/jobs/{job['id']}"

    async with app.state.pool.acquire() as conn:
        outbox = await conn.fetch("SELECT * FROM outbox WHERE job_id = $1", job["id"])
        events = await conn.fetch("SELECT * FROM job_events WHERE job_id = $1", job["id"])
    assert len(outbox) == 1 and outbox[0]["routing_key"] == "demo.sleep"
    assert outbox[0]["published_at"] is None
    assert [e["to_status"] for e in events] == ["queued"]


async def test_get_job_and_events(client, auth):
    job = (await client.post("/v1/jobs", json=sleep_job(), headers=auth)).json()
    got = await client.get(f"/v1/jobs/{job['id']}", headers=auth)
    assert got.status_code == 200 and got.json()["id"] == job["id"]
    events = (await client.get(f"/v1/jobs/{job['id']}/events", headers=auth)).json()
    assert events[0]["to_status"] == "queued" and events[0]["detail"] == {"reason": "submitted"}


async def test_jobs_are_isolated_between_users(client, auth):
    job = (await client.post("/v1/jobs", json=sleep_job(), headers=auth)).json()
    _, other = await make_user(client, email="mallory@example.com")
    assert (await client.get(f"/v1/jobs/{job['id']}", headers=other)).status_code == 404
    assert (await client.post(f"/v1/jobs/{job['id']}/cancel", headers=other)).status_code == 404
    assert (await client.get(f"/v1/jobs/{job['id']}/events", headers=other)).status_code == 404
    assert (await client.get("/v1/jobs", headers=other)).json()["items"] == []


async def test_unknown_job_is_404_and_bad_uuid_is_422(client, auth):
    assert (
        await client.get("/v1/jobs/00000000-0000-0000-0000-000000000000", headers=auth)
    ).status_code == 404
    assert (await client.get("/v1/jobs/not-a-uuid", headers=auth)).status_code == 422


async def test_validation_errors_point_at_the_field(client, auth):
    r = await client.post(
        "/v1/jobs", json={"type": "demo.sleep", "payload": {"seconds": -5}}, headers=auth
    )
    assert r.status_code == 422
    body = r.json()
    assert body["code"] == "invalid_payload"
    assert body["errors"][0]["loc"] == ["payload", "seconds"]

    bad_type = await client.post("/v1/jobs", json={"type": "nope", "payload": {}}, headers=auth)
    assert bad_type.status_code == 422 and bad_type.json()["code"] == "validation_error"


async def test_ssrf_payload_rejected_over_http(client, auth):
    r = await client.post(
        "/v1/jobs",
        json={"type": "video.transcode", "payload": {"source_url": "http://169.254.169.254/x"}},
        headers=auth,
    )
    assert r.status_code == 422


async def test_oversized_payload_is_413(client, auth):
    big = {"type": "ml.inference", "payload": {"model": "bert", "inputs": ["x" * 1000] * 300}}
    assert (await client.post("/v1/jobs", json=big, headers=auth)).status_code == 413


async def test_list_pagination_is_complete_ordered_and_stable(client, auth):
    ids = []
    for i in range(7):
        r = await client.post("/v1/jobs", json=sleep_job(payload={"seconds": i}), headers=auth)
        ids.append(r.json()["id"])

    seen, cursor, pages = [], None, 0
    while True:
        params = {"limit": 3, **({"cursor": cursor} if cursor else {})}
        page = (await client.get("/v1/jobs", params=params, headers=auth)).json()
        seen += [j["id"] for j in page["items"]]
        pages += 1
        cursor = page["next_cursor"]
        if not cursor:
            break
    assert pages == 3
    assert seen == list(reversed(ids))  # newest first, nothing skipped or duplicated


async def test_list_filters_by_status_and_rejects_bad_cursor(client, auth):
    a = (await client.post("/v1/jobs", json=sleep_job(payload={"seconds": 1}), headers=auth)).json()
    await client.post("/v1/jobs", json=sleep_job(payload={"seconds": 2}), headers=auth)
    await client.post(f"/v1/jobs/{a['id']}/cancel", headers=auth)
    cancelled = (await client.get("/v1/jobs", params={"status": "cancelled"}, headers=auth)).json()
    assert [j["id"] for j in cancelled["items"]] == [a["id"]]
    assert (await client.get("/v1/jobs", params={"cursor": "###"}, headers=auth)).status_code == 400


async def test_cancel_queued_job_then_conflict_on_second_cancel(client, auth):
    job = (await client.post("/v1/jobs", json=sleep_job(), headers=auth)).json()
    r = await client.post(f"/v1/jobs/{job['id']}/cancel", headers=auth)
    assert r.status_code == 200 and r.json()["status"] == "cancelled"
    assert r.json()["finished_at"] is not None
    again = await client.post(f"/v1/jobs/{job['id']}/cancel", headers=auth)
    assert again.status_code == 409 and again.json()["code"] == "not_cancellable"
    events = (await client.get(f"/v1/jobs/{job['id']}/events", headers=auth)).json()
    assert [e["to_status"] for e in events] == ["queued", "cancelled"]


async def test_cancel_running_job_is_cooperative(client, auth, app):
    job = (await client.post("/v1/jobs", json=sleep_job(), headers=auth)).json()
    async with app.state.pool.acquire() as conn:  # simulate a worker having claimed it
        await conn.execute("UPDATE jobs SET status = 'running' WHERE id = $1", job["id"])
    r = await client.post(f"/v1/jobs/{job['id']}/cancel", headers=auth)
    assert r.status_code == 202
    assert r.json()["status"] == "running" and r.json()["cancel_requested"] is True
