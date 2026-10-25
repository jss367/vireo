"""Calling contracts omit annotations, which have separate typing checks."""

import inspect


def calling_signature(function):
    """Keep parameter names, order, kinds and defaults while ignoring types."""
    signature = inspect.signature(function)
    return str(signature.replace(
        parameters=[
            parameter.replace(annotation=inspect.Parameter.empty)
            for parameter in signature.parameters.values()
        ],
        return_annotation=inspect.Signature.empty,
    ))
