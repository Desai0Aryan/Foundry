"""RabbitMQ topology and connection helpers.

    outbox relay --publish--> [foundry.jobs  direct exchange] --by job type--> (foundry.jobs queue,
                                                                              priority 0-9, durable)
                                                                                 |
                                       rejected / poison messages (broker dead-lettering)
                                                                                 v
    outbox relay --"dead"--> [foundry.dlx  direct exchange] ----------------> (foundry.poison queue)
                                                          \\---------------> (foundry.dead  queue)

* ``foundry.dead``   : application-level dead-letter queue. One message per job that exhausted
                       its retries, for alerting and inspection. State itself lives in Postgres
                       (``status = 'dead'``) and can be replayed through the API.
* ``foundry.poison`` : broker-level dead-lettering of messages that cannot even be parsed.
"""

from __future__ import annotations

from dataclasses import dataclass

import aio_pika
from aio_pika.abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractQueue,
    AbstractRobustConnection,
)

from .config import Settings
from .jobtypes import REGISTRY

DEAD_ROUTING_KEY = "dead"
POISON_ROUTING_KEY = "poison"


@dataclass(frozen=True)
class Names:
    prefix: str

    @property
    def jobs_exchange(self) -> str:
        return f"{self.prefix}.jobs"

    @property
    def jobs_queue(self) -> str:
        return f"{self.prefix}.jobs"

    @property
    def dlx(self) -> str:
        return f"{self.prefix}.dlx"

    @property
    def dead_queue(self) -> str:
        return f"{self.prefix}.dead"

    @property
    def poison_queue(self) -> str:
        return f"{self.prefix}.poison"


@dataclass
class Topology:
    jobs_exchange: AbstractExchange
    dlx: AbstractExchange
    jobs_queue: AbstractQueue
    dead_queue: AbstractQueue
    poison_queue: AbstractQueue


async def connect(settings: Settings) -> AbstractRobustConnection:
    return await aio_pika.connect_robust(settings.broker_url, timeout=10)


async def declare_topology(channel: AbstractChannel, settings: Settings) -> Topology:
    """Idempotent. Every process calls this on startup, so there is no separate provisioning."""
    n = Names(settings.broker_prefix)
    dlx = await channel.declare_exchange(n.dlx, aio_pika.ExchangeType.DIRECT, durable=True)
    dead = await channel.declare_queue(n.dead_queue, durable=True)
    poison = await channel.declare_queue(n.poison_queue, durable=True)
    await dead.bind(dlx, routing_key=DEAD_ROUTING_KEY)
    await poison.bind(dlx, routing_key=POISON_ROUTING_KEY)

    jobs_exchange = await channel.declare_exchange(
        n.jobs_exchange, aio_pika.ExchangeType.DIRECT, durable=True
    )
    jobs_queue = await channel.declare_queue(
        n.jobs_queue,
        durable=True,
        arguments={
            "x-max-priority": 9,
            "x-dead-letter-exchange": n.dlx,
            "x-dead-letter-routing-key": POISON_ROUTING_KEY,
        },
    )
    for job_type in REGISTRY:
        await jobs_queue.bind(jobs_exchange, routing_key=job_type)
    return Topology(jobs_exchange, dlx, jobs_queue, dead, poison)


async def delete_topology(channel: AbstractChannel, settings: Settings) -> None:
    """Used by tests to clean up their uniquely-prefixed topology."""
    n = Names(settings.broker_prefix)
    for q in (n.jobs_queue, n.dead_queue, n.poison_queue):
        await channel.queue_delete(q)
    for e in (n.jobs_exchange, n.dlx):
        await channel.exchange_delete(e)
