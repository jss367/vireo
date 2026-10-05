"""Prove separation from application imports, wheel, and executable archives."""

import ast
import importlib.util
import shutil
import subprocess
import sys
import zipfile

import pytest
from conftest import REPO
from setuptools import find_namespace_packages


def test_tool_is_outside_application_package_and_imports():
    packages = find_namespace_packages(where=str(REPO), include=["vireo*"])
    assert not any("encounter_eval" in name for name in packages)
    for path in (REPO / "vireo").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert all(not item.name.startswith("encounter_eval") for item in node.names), path
            elif isinstance(node, ast.ImportFrom):
                assert not (node.module or "").startswith("encounter_eval"), path


def test_application_wheel_excludes_evaluation_package(tmp_path):
    # setuptools writes build/lib under its source root. Keep the wheel test out
    # of the checkout's build/ directory, which Tauri uses as frontend assets.
    source = tmp_path / "source"
    source.mkdir()
    shutil.copy2(REPO / "pyproject.toml", source / "pyproject.toml")
    shutil.copytree(REPO / "vireo", source / "vireo", ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copytree(REPO / "tools" / "encounter-evaluation", source / "tools" / "encounter-evaluation",
                    ignore=shutil.ignore_patterns("__pycache__", "*.egg-info", ".pytest_cache"))
    result = subprocess.run([sys.executable, "-m", "build", "--wheel", "--no-isolation",
                    "--outdir", str(tmp_path), str(source)], capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    wheel = next(tmp_path.glob("*.whl"))
    with zipfile.ZipFile(wheel) as archive:
        assert not any("encounter_eval" in name or "encounter-evaluation" in name for name in archive.namelist())


def test_archive_guard_reads_embedded_modules(tmp_path):
    spec = importlib.util.spec_from_file_location("build_sidecar", REPO / "scripts" / "build_sidecar.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    # Real tiny executable with an intentionally bundled tool-name module.
    # It needs no Vireo runtime, models, signing credentials, or private data.
    (tmp_path / "encounter_eval_marker.py").write_text("value = 1\n")
    entry = tmp_path / "entry.py"
    entry.write_text("import encounter_eval_marker\nprint(encounter_eval_marker.value)\n")
    subprocess.run([sys.executable, "-m", "PyInstaller", "--onefile", "--noconfirm", "--log-level", "ERROR",
                    "--distpath", str(tmp_path / "dist"), "--workpath", str(tmp_path / "build"),
                    "--specpath", str(tmp_path), str(entry)], cwd=tmp_path, check=True, capture_output=True, text=True)
    executable = tmp_path / "dist" / ("entry.exe" if sys.platform == "win32" else "entry")
    with pytest.raises(RuntimeError, match="leaked"):
        module.assert_no_evaluation_modules(executable)


def test_build_always_excludes_tool():
    tree = ast.parse((REPO / "scripts" / "build_sidecar.py").read_text())
    args = next(node.value for node in ast.walk(tree) if isinstance(node, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == "pyinstaller_args" for t in node.targets))
    values = [node.value if isinstance(node, ast.Constant) else None for node in args.elts]
    assert any(a == "--exclude-module" and b == "encounter_eval" for a, b in zip(values, values[1:], strict=False))


def test_evaluation_wheel_includes_review_page_and_command(tmp_path):
    source = tmp_path / 'source'
    shutil.copytree(REPO / 'tools' / 'encounter-evaluation', source,
                    ignore=shutil.ignore_patterns('__pycache__', '*.egg-info', '.pytest_cache', 'build'))
    result = subprocess.run([sys.executable, '-m', 'build', '--wheel', '--no-isolation',
                             '--outdir', str(tmp_path), str(source)], capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    with zipfile.ZipFile(next(tmp_path.glob('*.whl'))) as archive:
        assert 'encounter_eval/review.html' in archive.namelist()
        assert 'encounter_eval/continuity_review.html' in archive.namelist()
        assert 'encounter_eval/merge_audit.html' in archive.namelist()
        entrypoints = next(name for name in archive.namelist() if name.endswith('/entry_points.txt'))
        assert 'vireo-review-encounters = encounter_eval.review_server:main' in archive.read(entrypoints).decode()
