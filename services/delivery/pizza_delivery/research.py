"""Shim entry point: `python -m pizza_delivery.research` → research_cli.main()"""

from pizza_delivery.research_cli import main


if __name__ == "__main__":
    raise SystemExit(main())
