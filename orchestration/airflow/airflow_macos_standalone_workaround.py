"""macOS workaround for Airflow standalone log-server crashes.

Airflow starts small Gunicorn log servers for the scheduler and triggerer.
On macOS, those forked child processes can crash inside `setproctitle()`.
"""

import sys


def apply() -> None:
    if sys.platform != "darwin":
        return

    try:
        import setproctitle
    except Exception:
        return

    def _noop_setproctitle(_title: str) -> None:
        return None

    setproctitle.setproctitle = _noop_setproctitle
