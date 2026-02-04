import sys
from pathlib import Path

# Dans la CI : repo monté sur /opt/airflow
SCRIPTS_DIR = Path("/opt/airflow/airflow/scripts")

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
