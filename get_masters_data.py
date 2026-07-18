"""Compatibility shim — use ``python -m market_data.download_masters``."""

from market_data.download_masters import main

if __name__ == "__main__":
    main()
