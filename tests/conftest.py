"""Isola o Airflow do ambiente antes de qualquer import dele."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent

os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow-test-"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION", "True")
