"""Signature normalization ignores types without weakening calling contracts."""

import inspect

from signature_contract import calling_signature


def test_calling_signature_ignores_annotations_without_changing_the_function():
    def annotated(first: int, /, second: str = "value", *items: float,
                  enabled: bool = True, **options: object) -> None:
        pass

    original = inspect.signature(annotated)
    assert calling_signature(annotated) == "(first, /, second='value', *items, enabled=True, **options)"
    assert inspect.signature(annotated) == original
    assert original.return_annotation is None


def test_calling_signature_preserves_all_parameter_contract_details():
    def original(first, second=1, *, enabled=True):
        pass

    signature = inspect.signature(original)
    for parameters in [
        [parameter.replace(name="renamed") if parameter.name == "first" else parameter
         for parameter in signature.parameters.values()],
        [parameter.replace(default=2) if parameter.name == "second" else parameter
         for parameter in signature.parameters.values()],
        [parameter.replace(kind=inspect.Parameter.POSITIONAL_ONLY) if parameter.name == "first" else parameter
         for parameter in signature.parameters.values()],
        [parameter.replace(kind=inspect.Parameter.POSITIONAL_OR_KEYWORD) if parameter.name == "enabled" else parameter
         for parameter in signature.parameters.values()],
        [*list(signature.parameters.values())[:1], *list(signature.parameters.values())[2:]],
    ]:
        def changed():
            pass

        changed.__signature__ = signature.replace(parameters=parameters)
        assert calling_signature(changed) != calling_signature(original)
