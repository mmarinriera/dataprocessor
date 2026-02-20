from collections.abc import Callable
from typing import Any
from typing import get_type_hints


class ValidationError(Exception):
    """Pipeline step validation has failed."""


def get_return_type_annotation(func: Callable[..., Any]) -> Any:
    return get_type_hints(func).get("return", None)


def get_arg_type_annotations(func: Callable[..., Any]) -> dict[str, Any]:
    type_hints = get_type_hints(func)
    type_hints.pop("return", None)
    return type_hints
