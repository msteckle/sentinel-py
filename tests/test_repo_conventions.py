"""Structural conventions from AGENTS.md, enforced so they cannot silently regress.

Import hygiene and likely-bug patterns are handled by ``ruff`` (see ``pyproject.toml``);
these tests cover the project-specific rules ruff cannot express.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[1] / "src" / "sentinel_py"
MODULES = sorted(path for path in SRC.rglob("*.py") if path.name != "__init__.py")
ENUM_BASES = {"Enum", "StrEnum", "IntEnum", "Flag", "IntFlag"}


def _parse(path: Path) -> ast.Module:
    return ast.parse(path.read_text())


def _label(path: Path) -> str:
    return str(path.relative_to(SRC))


@pytest.mark.parametrize("path", MODULES, ids=_label)
def test_every_module_has_a_docstring(path: Path):
    assert ast.get_docstring(_parse(path)), f"{_label(path)} needs a module docstring"


def test_public_functions_classes_and_methods_have_docstrings():
    """Public API (module level and methods of module-level classes) is documented."""
    missing: list[str] = []
    for path in MODULES:
        for node in _parse(path).body:
            if isinstance(node, ast.ClassDef):
                members = [node] + [
                    child
                    for child in node.body
                    if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
                ]
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                members = [node]
            else:
                continue
            for member in members:
                is_dunder = member.name.startswith("__") and member.name.endswith("__")
                if member.name.startswith("_") or is_dunder:
                    continue
                if not ast.get_docstring(member):
                    missing.append(f"{_label(path)}:{member.lineno} {member.name}")
    assert not missing, "Missing docstrings:\n" + "\n".join(missing)


def test_enums_are_defined_only_in_enums_module():
    """Controlled vocabularies live in ``sentinel_py/enums.py`` (or one declared home)."""
    offenders: list[str] = []
    for path in MODULES:
        if path.name == "enums.py" and path.parent == SRC:
            continue
        for node in ast.walk(_parse(path)):
            if not isinstance(node, ast.ClassDef):
                continue
            base_names = {
                base.id if isinstance(base, ast.Name) else getattr(base, "attr", "")
                for base in node.bases
            }
            if base_names & ENUM_BASES:
                offenders.append(f"{_label(path)}:{node.lineno} {node.name}")
    assert not offenders, "Enums outside enums.py:\n" + "\n".join(offenders)


def test_library_code_logs_instead_of_printing():
    """Only CLI modules write to the console; libraries use loggers."""
    offenders: list[str] = []
    for path in MODULES:
        if "cli" in path.relative_to(SRC).parts:
            continue
        for node in ast.walk(_parse(path)):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "print"
            ):
                offenders.append(f"{_label(path)}:{node.lineno}")
    assert not offenders, "print() in library code:\n" + "\n".join(offenders)


def test_processors_declare_a_version_and_are_registered():
    """Every processor module defines ``PROCESSOR_VERSION`` and is built in."""
    from sentinel_py.pipeline.processors import get_default_registry

    registered = {
        type(get_default_registry().get(name)).__name__
        for name in get_default_registry().type_names
    }
    processor_dir = SRC / "pipeline" / "processors"
    for path in sorted(processor_dir.glob("*.py")):
        tree = _parse(path)
        classes = [
            node.name
            for node in tree.body
            if isinstance(node, ast.ClassDef)
            and any(
                isinstance(base, ast.Name) and base.id == "BaseProcessor"
                for base in node.bases
            )
        ]
        if not classes:
            continue
        constants = {
            target.id
            for node in tree.body
            if isinstance(node, ast.Assign)
            for target in node.targets
            if isinstance(target, ast.Name)
        }
        assert "PROCESSOR_VERSION" in constants, f"{_label(path)} lacks PROCESSOR_VERSION"
        for name in classes:
            assert name in registered, f"{name} is not registered as a built-in"
