# worker.py
import asyncio
import os
import signal

import bridge_buyer_backend as appmod  # imports your FastAPI module

# we rely on @startup hooks defined in the module to init DB, pools, crons, etc.

# Minimal loop that only starts the sequencer + background jobs (already registered by @startup)
# FastAPI lifespan isn't needed for worker; we just import the module so its @startup runs.
# Ensure ENV=production so CSRF & TrustedHost etc behave the same across pods.

async def main():
    # Explicitly kick the sequencer if not already started
    try:
        await appmod._start_sequencer()  # safe idempotent
    except Exception:
        pass

    # Keep process alive forever until SIGTERM
    stop = asyncio.Event()

    def _sig(*_):
        stop.set()
    for s in (signal.SIGINT, signal.SIGTERM):
        signal.signal(s, _sig)

    await stop.wait()

if __name__ == "__main__":
    asyncio.run(main())
