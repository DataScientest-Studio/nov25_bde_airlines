import os
import types
from pathlib import Path

# on évite ValueError sur AIRLABS_API_KEY
os.environ["AIRLABS_API_KEY"] = "DUMMY_FOR_TESTS"

def load_collect_module_without_running():
    """
    Charge Collect_data.py en n'exécutant que la partie 'fonctions',
    en coupant avant la logique runtime (collecte).
    """
    script_path = Path("/opt/airflow/airflow/scripts/Collect_data.py")
    code = script_path.read_text(encoding="utf-8")

    # on coupe le fichier avant la collecte runtime.
    # dans le script, la collecte commence à: print("✈️ COLLECTE DES VOLS")
    marker = 'print("✈️ COLLECTE DES VOLS")'
    idx = code.find(marker)
    assert idx != -1, "Marker not found in Collect_data.py (script structure changed)"

    code_defs_only = code[:idx]

    module = types.ModuleType("Collect_data_partial")
    exec(code_defs_only, module.__dict__)
    return module


def test_is_quota_error_true():
    m = load_collect_module_without_running()
    data = {"error": {"code": "month_limit_exceeded"}}
    assert m.is_quota_error(data) is True


def test_normalize_response_dict():
    m = load_collect_module_without_running()
    data = {"response": {"a": 1}}
    assert m.normalize_response(data) == [{"a": 1}]
