import sys
from pathlib import Path

SCRIPTS_DIR = Path("/opt/airflow/scripts")

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

