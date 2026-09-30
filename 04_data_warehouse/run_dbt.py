"""Runs dbt for the telematics project with the settings from the repo-root .env.

dbt reads its connection settings from environment variables (see profiles.yml), but does not load
.env files itself. This wrapper loads .env (without overriding variables already set) and runs dbt
from the project directory, so it works from any working directory on Windows, macOS and Linux.

Usage (from the repo root):
    python 04_data_warehouse/run_dbt.py deps
    python 04_data_warehouse/run_dbt.py build
    python 04_data_warehouse/run_dbt.py test --select silver
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_DIR = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_DIR.parent


def dbt_executable() -> str:
    """dbt from the same environment as this interpreter, falling back to PATH."""
    scripts = Path(sys.executable).parent
    for name in ("dbt.exe", "dbt"):
        if (scripts / name).exists():
            return str(scripts / name)
    found = shutil.which("dbt")
    if not found:
        sys.exit("dbt not found: pip install -r 04_data_warehouse/requirements.txt")
    return found


def main() -> int:
    load_dotenv(REPO_ROOT / ".env")
    env = {**os.environ, "DBT_PROFILES_DIR": str(PROJECT_DIR), "DBT_PROJECT_DIR": str(PROJECT_DIR)}
    return subprocess.call([dbt_executable(), *sys.argv[1:]], cwd=PROJECT_DIR, env=env)


if __name__ == "__main__":
    sys.exit(main())
