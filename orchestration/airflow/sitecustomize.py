"""Airflow startup hook for local repository-specific runtime fixes.

The filename must stay exactly `sitecustomize.py` so Python auto-loads it
during interpreter startup when `orchestration/airflow/` is on `PYTHONPATH`.
"""

from airflow_macos_standalone_workaround import apply


apply()
