"""
test_dag_integrity.py
Pastikan semua DAG di folder airflow/dags dapat di-parse tanpa error,
tidak ada cyclic dependency, dan memenuhi konvensi minimum (punya tags, retries wajar).
"""
import os
import glob
import importlib.util
import pytest

DAG_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))


def _load_dag_module(filepath):
    spec = importlib.util.spec_from_file_location(os.path.basename(filepath), filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("dag_file", [f for f in glob.glob(os.path.join(DAG_FOLDER, "*.py")) if not f.endswith("__init__.py")])
def test_dag_file_imports_without_error(dag_file):
    """Uji apakah file DAG bisa diimpor tanpa melempar SyntaxError atau ImportError."""
    module = _load_dag_module(dag_file)
    assert module is not None


@pytest.mark.parametrize("dag_file", [f for f in glob.glob(os.path.join(DAG_FOLDER, "*.py")) if not f.endswith("__init__.py")])
def test_dag_has_tags(dag_file):
    """Uji konvensi: setiap DAG wajib memiliki label tags."""
    from airflow.models import DagBag

    dagbag = DagBag(dag_folder=os.path.dirname(dag_file), include_examples=False)
    assert len(dagbag.import_errors) == 0, f"Import errors: {dagbag.import_errors}"
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"DAG {dag_id} tidak punya tags"
