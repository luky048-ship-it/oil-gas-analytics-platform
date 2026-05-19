"""
DAG Integrity Tests for Airflow Pipeline Layer.

These tests verify that Airflow can successfully parse all DAG files
without syntax errors, import errors, or structural issues.

Tests cover:
- Syntax validation (Python can compile the file)
- Import validation (all modules can be imported)
- DAG structure validation (DAG object is properly defined)
- Task validation (all tasks are properly configured)
"""

import os
import pytest
from pathlib import Path


# Path to the DAGs directory
DAGS_DIR = Path(__file__).parent.parent.parent / "dags"
PLUGINS_DIR = Path(__file__).parent.parent.parent / "plugins"


def get_dag_files():
    """Get all Python files in the DAGs directory."""
    dag_files = []
    for file_path in DAGS_DIR.glob("*.py"):
        if not file_path.name.startswith("_"):
            dag_files.append(file_path)
    return dag_files


def get_plugin_modules():
    """Get all plugin modules that should be importable."""
    plugin_modules = []
    for root, dirs, files in os.walk(PLUGINS_DIR):
        root_path = Path(root)
        # Skip __pycache__ directories
        if "__pycache__" in str(root_path):
            continue
        for file_name in files:
            if file_name.endswith(".py") and not file_name.startswith("_"):
                file_path = Path(file_name)
                module_path = root_path.relative_to(PLUGINS_DIR.parent) / file_path.stem
                plugin_modules.append(str(module_path).replace(os.sep, "."))
    return plugin_modules


class TestDagIntegrity:
    """Test class for DAG integrity validation."""

    @pytest.mark.parametrize("dag_file", get_dag_files(), ids=lambda x: x.name)
    def test_dag_syntax_valid(self, dag_file):
        """
        Test that each DAG file has valid Python syntax.
        
        This test attempts to compile each DAG file to ensure there are
        no syntax errors.
        """
        with open(dag_file, "r", encoding="utf-8") as f:
            source_code = f.read()
        
        # This will raise SyntaxError if there are syntax issues
        try:
            compile(source_code, str(dag_file), "exec")
        except SyntaxError as e:
            pytest.fail(f"Syntax error in {dag_file.name}: {e}")

    @pytest.mark.parametrize("dag_file", get_dag_files(), ids=lambda x: x.name)
    def test_dag_imports_valid(self, dag_file, monkeypatch):
        """
        Test that each DAG file can be imported without errors.
        
        This test verifies that all imports in the DAG files resolve correctly,
        including imports from the plugins directory.
        """
        # Add plugins directory to Python path
        plugins_path = str(PLUGINS_DIR.parent)
        dags_path = str(DAGS_DIR.parent)
        
        if plugins_path not in [str(p) for p in __import__('sys').path]:
            monkeypatch.syspath_prepend(plugins_path)
        if dags_path not in [str(p) for p in __import__('sys').path]:
            monkeypatch.syspath_prepend(dags_path)
        
        # Import the module
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            dag_file.stem, dag_file
        )
        
        try:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        except ImportError as e:
            pytest.fail(f"Import error in {dag_file.name}: {e}")
        except Exception as e:
            pytest.fail(f"Error loading {dag_file.name}: {e}")

    @pytest.mark.parametrize("dag_file", get_dag_files(), ids=lambda x: x.name)
    def test_dag_object_defined(self, dag_file, monkeypatch):
        """
        Test that each DAG file defines at least one DAG object.
        
        This test ensures that each file contains a properly configured
        Airflow DAG instance.
        """
        # Add plugins directory to Python path
        plugins_path = str(PLUGINS_DIR.parent)
        dags_path = str(DAGS_DIR.parent)
        
        if plugins_path not in [str(p) for p in __import__('sys').path]:
            monkeypatch.syspath_prepend(plugins_path)
        if dags_path not in [str(p) for p in __import__('sys').path]:
            monkeypatch.syspath_prepend(dags_path)
        
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            dag_file.stem, dag_file
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Check for DAG objects in module
        from airflow import DAG
        from airflow.decorators import dag
        
        dag_objects = []
        for name, obj in vars(module).items():
            if isinstance(obj, DAG):
                dag_objects.append(name)
            elif callable(obj) and hasattr(obj, 'dag_id'):
                # Check for @dag decorated functions
                dag_objects.append(name)
        
        assert len(dag_objects) > 0, f"No DAG objects found in {dag_file.name}"

    @pytest.mark.parametrize("dag_file", get_dag_files(), ids=lambda x: x.name)
    def test_dag_has_required_attributes(self, dag_file, monkeypatch):
        """
        Test that each DAG has required attributes configured.
        
        Required attributes:
        - dag_id
        - schedule
        - start_date
        """
        # Add plugins directory to Python path
        plugins_path = str(PLUGINS_DIR.parent)
        dags_path = str(DAGS_DIR.parent)
        
        if plugins_path not in [str(p) for p in __import__('sys').path]:
            monkeypatch.syspath_prepend(plugins_path)
        if dags_path not in [str(p) for p in __import__('sys').path]:
            monkeypatch.syspath_prepend(dags_path)
        
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            dag_file.stem, dag_file
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        from airflow import DAG
        
        dag_objects = []
        for name, obj in vars(module).items():
            if isinstance(obj, DAG):
                dag_objects.append(obj)
        
        for dag_obj in dag_objects:
            assert hasattr(dag_obj, 'dag_id') and dag_obj.dag_id, \
                f"DAG in {dag_file.name} missing dag_id"
            assert hasattr(dag_obj, 'schedule') or hasattr(dag_obj, 'schedule_interval'), \
                f"DAG '{dag_obj.dag_id}' in {dag_file.name} missing schedule"
            assert hasattr(dag_obj, 'start_date') and dag_obj.start_date, \
                f"DAG '{dag_obj.dag_id}' in {dag_file.name} missing start_date"

    @pytest.mark.parametrize("dag_file", get_dag_files(), ids=lambda x: x.name)
    def test_dag_tasks_valid(self, dag_file, monkeypatch):
        """
        Test that DAG tasks are properly configured.
        
        This test verifies that:
        - All tasks have unique task_ids within the DAG
        - Tasks are properly attached to the DAG
        """
        # Add plugins directory to Python path
        plugins_path = str(PLUGINS_DIR.parent)
        dags_path = str(DAGS_DIR.parent)
        
        if plugins_path not in [str(p) for p in __import__('sys').path]:
            monkeypatch.syspath_prepend(plugins_path)
        if dags_path not in [str(p) for p in __import__('sys').path]:
            monkeypatch.syspath_prepend(dags_path)
        
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            dag_file.stem, dag_file
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        from airflow import DAG
        from airflow.models.baseoperator import BaseOperator
        
        dag_objects = {}
        for name, obj in vars(module).items():
            if isinstance(obj, DAG):
                dag_objects[obj.dag_id] = obj
        
        for dag_id, dag_obj in dag_objects.items():
            task_ids = set()
            for task in dag_obj.tasks:
                # Check for duplicate task_ids
                assert task.task_id not in task_ids, \
                    f"Duplicate task_id '{task.task_id}' in DAG '{dag_id}' ({dag_file.name})"
                task_ids.add(task.task_id)
                
                # Verify task is properly configured
                assert isinstance(task, BaseOperator), \
                    f"Task '{task.task_id}' in DAG '{dag_id}' is not a valid operator"


class TestPluginImports:
    """Test class for plugin module import validation."""

    @pytest.mark.parametrize("plugin_module", get_plugin_modules(), ids=lambda x: x.split(".")[-1])
    def test_plugin_importable(self, plugin_module, monkeypatch):
        """
        Test that each plugin module can be imported.
        
        This ensures that all custom plugins are properly structured
        and can be loaded by Airflow.
        """
        plugins_path = str(PLUGINS_DIR.parent)
        
        if plugins_path not in [str(p) for p in __import__('sys').path]:
            monkeypatch.syspath_prepend(plugins_path)
        
        try:
            __import__(plugin_module)
        except ImportError as e:
            pytest.fail(f"Cannot import plugin module '{plugin_module}': {e}")
        except Exception as e:
            pytest.fail(f"Error importing plugin module '{plugin_module}': {e}")


class TestDagDependencies:
    """Test class for DAG dependency validation."""

    def test_all_dags_parseable(self):
        """
        Integration test that verifies all DAGs can be parsed together.
        
        This simulates what Airflow does when scanning the DAGs folder,
        ensuring there are no cross-file conflicts or issues.
        """
        import sys
        plugins_path = str(PLUGINS_DIR.parent)
        dags_path = str(DAGS_DIR.parent)
        
        if plugins_path not in [str(p) for p in sys.path]:
            sys.path.insert(0, plugins_path)
        if dags_path not in [str(p) for p in sys.path]:
            sys.path.insert(0, dags_path)
        
        dag_files = get_dag_files()
        parsed_count = 0
        
        for dag_file in dag_files:
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                dag_file.stem, dag_file
            )
            module = importlib.util.module_from_spec(spec)
            
            try:
                spec.loader.exec_module(module)
                parsed_count += 1
            except Exception as e:
                pytest.fail(f"Failed to parse {dag_file.name}: {e}")
        
        assert parsed_count == len(dag_files), \
            f"Not all DAG files were successfully parsed ({parsed_count}/{len(dag_files)})"
