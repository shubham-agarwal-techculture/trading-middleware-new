"""Compatibility shim — use ``run_oms``."""

from run_oms import main

if __name__ == "__main__":
    import asyncio
    from run_oms import main as _main

    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass
