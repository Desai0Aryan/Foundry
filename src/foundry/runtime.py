from __future__ import annotations

import asyncio
import contextlib
import signal


def install_stop_signals(stop: asyncio.Event) -> None:
    """SIGTERM (Kubernetes/Docker) and SIGINT (Ctrl-C) trigger a graceful stop."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):  # not available on Windows
            loop.add_signal_handler(sig, stop.set)


async def sleep_or_stop(stop: asyncio.Event, seconds: float) -> bool:
    """Sleep up to `seconds`; returns True if a stop was requested meanwhile."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
        return True
    except TimeoutError:
        return False
