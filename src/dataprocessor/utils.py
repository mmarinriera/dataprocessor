import inspect
from collections.abc import Callable
from typing import Any
from typing import get_type_hints


class ValidationError(Exception):
    """Pipeline step validation has failed."""


def get_func_return_type_annotation(func: Callable[..., Any]) -> Any:
    return get_type_hints(func).get("return", None)


def get_func_arg_type_annotations(func: Callable[..., Any]) -> dict[str, Any]:
    type_hints = get_type_hints(func)
    type_hints.pop("return", None)
    return type_hints


def get_func_required_args(func: Callable[..., Any]) -> list[str]:
    sig = inspect.signature(func)
    required_args = []
    for arg_name, param in sig.parameters.items():
        if param.default is inspect.Parameter.empty:
            required_args.append(arg_name)
    return required_args
