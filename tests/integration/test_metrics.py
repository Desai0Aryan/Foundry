import pytest
from prometheus_client import REGISTRY

from foundry import metrics
from tests.conftest import make_client, make_user, set_limits, sleep_job

pytestmark = pytest.mark.integration


def sample(name: str, **labels) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


async def test_metrics_endpoint_exposes_prometheus_text(client, app):
    _, auth = await make_user(client)
    await client.get("/v1/me", headers=auth)
    r = await client.get("/metrics")
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/plain")
    assert "# TYPE foundry_http_requests_total counter" in r.text
    assert "# TYPE foundry_job_end_to_end_seconds histogram" in r.text
    assert "# TYPE foundry_jobs gauge" in r.text


async def test_http_metrics_use_route_templates_not_raw_urls(client, app):
    _, auth = await make_user(client)
    created = (await client.post("/v1/jobs", json=sleep_job(), headers=auth)).json()
    before = sample(
        "foundry_http_requests_total", method="GET", route="/v1/jobs/{job_id}", status="200"
    )
    await client.get(f"/v1/jobs/{created['id']}", headers=auth)
    assert (
        sample("foundry_http_requests_total", method="GET", route="/v1/jobs/{job_id}", status="200")
        == before + 1
    )
    text = (await client.get("/metrics")).text
    assert created["id"] not in text  # no per-job label explosion
    await client.get("/definitely/not/a/route")
    assert 'route="unmatched"' in (await client.get("/metrics")).text
    assert (
        sample(
            "foundry_http_request_duration_seconds_count", method="GET", route="/v1/jobs/{job_id}"
        )
        >= 1
    )


async def test_submission_outcomes_are_counted(client, app):
    _, auth = await make_user(client)
    base = {
        o: sample("foundry_job_submissions_total", outcome=o)
        for o in ("created", "replayed", "deduplicated")
    }
    h = {**auth, "Idempotency-Key": "m1"}
    await client.post("/v1/jobs", json=sleep_job(), headers=h)
    await client.post("/v1/jobs", json=sleep_job(), headers=h)
    d = sleep_job(deduplicate=True, payload={"seconds": 9})
    await client.post("/v1/jobs", json=d, headers=auth)
    await client.post("/v1/jobs", json=d, headers=auth)
    for outcome, delta in {"created": 2, "replayed": 1, "deduplicated": 1}.items():
        assert sample("foundry_job_submissions_total", outcome=outcome) - base[outcome] == delta


async def test_system_gauges_reflect_postgres(client, app):
    user, auth = await make_user(client)
    await set_limits(client, user["id"], rate_limit_per_sec=1000, max_concurrent_jobs=100)
    for i in range(3):
        await client.post("/v1/jobs", json=sleep_job(payload={"seconds": i}), headers=auth)
    async with app.state.pool.acquire() as c:
        await c.execute("UPDATE jobs SET status='dead' WHERE id = (SELECT id FROM jobs LIMIT 1)")
        await c.execute(
            "UPDATE jobs SET queued_at = now() - interval '90 seconds' WHERE status='queued'"
        )
    await metrics.refresh_system_gauges(app.state.pool, 0)
    assert sample("foundry_jobs", status="queued") == 2
    assert sample("foundry_jobs", status="dead") == 1
    assert sample("foundry_jobs", status="running") == 0  # absent statuses are 0, not missing
    assert 89 <= sample("foundry_queue_oldest_age_seconds") <= 95
    assert sample("foundry_outbox_backlog") == 3
    assert sample("foundry_workers_alive") == 0


async def test_system_gauge_refresh_is_cached_between_scrapes(client, app):
    await metrics.refresh_system_gauges(app.state.pool, 0)
    _, auth = await make_user(client)
    await client.post("/v1/jobs", json=sleep_job(), headers=auth)
    await metrics.refresh_system_gauges(app.state.pool, 60)  # within cache window: no re-query
    assert sample("foundry_jobs", status="queued") == 0
    await metrics.refresh_system_gauges(app.state.pool, 0)
    assert sample("foundry_jobs", status="queued") == 1


async def test_metrics_can_require_a_token(client, app):
    async with make_client(metrics_token="s3cret") as c2:
        assert (await c2.get("/metrics")).status_code == 401
        assert (
            await c2.get("/metrics", headers={"Authorization": "Bearer nope"})
        ).status_code == 401
        ok = await c2.get("/metrics", headers={"Authorization": "Bearer s3cret"})
        assert ok.status_code == 200 and "foundry_jobs" in ok.text
