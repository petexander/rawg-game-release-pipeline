"""Module entrypoint for ``python -m game_release_pipeline``."""

from .cli import main


if __name__ == "__main__":
    raise SystemExit(main())
