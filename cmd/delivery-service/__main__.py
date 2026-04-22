"""Entry point shim that delegates to pizza_delivery.__main__."""

from pizza_delivery.__main__ import main

if __name__ == "__main__":
    raise SystemExit(main())
