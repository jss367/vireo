"""Every function in a covered directory is fully annotated.

basedpyright checks a function's body either way, but it can only check the
calls *into* a function, and what the function hands back, when the
signature says what it takes and returns. ``COVERED_ROOTS`` lists the
directories under ``vireo/`` that are held at 100%: every function and
method there, nested ones included, annotates each parameter (a method's
leading ``self``/``cls`` excepted) and declares its return type (``-> None``
when it returns nothing). Lambdas cannot carry annotations and are skipped.

To cover another directory, annotate it and add it to ``COVERED_ROOTS``.
There is no baseline: a root is either covered completely or not listed.
"""

import ast
from pathlib import Path

import pytest

VIREO = Path(__file__).resolve().parents[1]

# Directories under vireo/ whose functions must be fully annotated.
COVERED_ROOTS = ("services", "pipeline_stages", "classify_job")


def _first_param_is_implicit(node: ast.FunctionDef | ast.AsyncFunctionDef, in_class: bool) -> bool:
    """A method's first parameter (``self``/``cls``) is bound, not annotated."""
    if not in_class:
        return False
    return not any(
        (isinstance(d, ast.Name) and d.id == "staticmethod")
        or (
            isinstance(d, ast.Attribute)
            and d.attr == "staticmethod"
            and isinstance(d.value, ast.Name)
            and d.value.id == "builtins"
        )
        for d in node.decorator_list
    )


def missing_annotations(source: str) -> list[tuple[str, list[str]]]:
    """``[(qualname, missing)]`` for each function in ``source`` lacking an annotation.

    ``missing`` names each unannotated parameter, plus ``return`` when the
    return type is missing.
    """
    found: list[tuple[str, list[str]]] = []

    def visit(node: ast.AST, stack: list[str], in_class: bool) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.ClassDef):
                visit(child, [*stack, child.name], True)
            elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                args = child.args
                params = [*args.posonlyargs, *args.args]
                if params and _first_param_is_implicit(child, in_class):
                    params = params[1:]
                params += args.kwonlyargs
                if args.vararg is not None:
                    params.append(args.vararg)
                if args.kwarg is not None:
                    params.append(args.kwarg)
                missing = [p.arg for p in params if p.annotation is None]
                if child.returns is None:
                    missing.append("return")
                qualname = ".".join([*stack, child.name])
                if missing:
                    found.append((qualname, missing))
                visit(child, [*stack, child.name], False)
            else:
                visit(child, stack, in_class)

    visit(ast.parse(source), [], False)
    return found


@pytest.mark.parametrize("root", COVERED_ROOTS)
def test_covered_root_is_fully_annotated(root):
    directory = VIREO / root
    assert directory.is_dir(), f"covered root vireo/{root} does not exist; update COVERED_ROOTS"
    problems = []
    for path in sorted(directory.rglob("*.py")):
        rel = path.relative_to(VIREO).as_posix()
        for qualname, missing in missing_annotations(path.read_text(encoding="utf-8")):
            problems.append(f"{rel}:{qualname} (missing: {', '.join(missing)})")
    assert not problems, (
        f"Functions under vireo/{root} must annotate every parameter and the "
        "return type:\n    " + "\n    ".join(problems)
    )


def test_missing_annotations_reports_each_gap():
    source = '''
def full(a: int, *args: str, b: int = 0, **kw: object) -> None: ...
def bare(a, b: int): ...
class C:
    def method(self, x: int) -> int:
        def inner(y): return y
        return inner(x)
    @classmethod
    def build(cls) -> "C": ...
    @staticmethod
    def helper(self) -> None: ...
    def untyped_kwargs(self, **kw) -> None: ...
'''
    assert missing_annotations(source) == [
        ("bare", ["a", "return"]),
        ("C.method.inner", ["y", "return"]),
        ("C.helper", ["self"]),
        ("C.untyped_kwargs", ["kw"]),
    ]


@pytest.mark.parametrize("decorator", ["staticmethod", "builtins.staticmethod"])
@pytest.mark.parametrize("kind", ["def", "async def"])
def test_staticmethod_first_parameter_requires_annotation(decorator, kind):
    source = f"class C:\n    @{decorator}\n    {kind} helper(self, /, value: int) -> None: ...\n"
    assert missing_annotations(source) == [("C.helper", ["self"])]
    source = source.replace("helper(self,", "helper(self: object,")
    assert missing_annotations(source) == []


def test_qualified_classmethod_keeps_bound_parameter_exemption():
    source = "class C:\n    @builtins.classmethod\n    def build(cls, value: int) -> None: ...\n"
    assert missing_annotations(source) == []
