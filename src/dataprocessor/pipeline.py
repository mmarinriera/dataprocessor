from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from graphlib import TopologicalSorter
from pathlib import Path
from typing import Any


@dataclass
class Step:
    name: str
    processor: Callable[..., Any]
    inputs: list[str] = field(default_factory=list)
    params: dict[str, Any] = field(default_factory=dict)
    input_data: Any = None
    data: Any | None = field(init=False, default=None)
    save_method: Callable[[Any, str | Path], None] | None = None
    save_path: str | Path | None = None


class Pipeline:
    """A simple data processing pipeline that allows you to define steps with dependencies and execute them in the correct order."""

    def __init__(self) -> None:
        self.steps: dict[str, Step] = {}
        self.sorter: TopologicalSorter[str] = TopologicalSorter()

    def add_step(
        self,
        name: str,
        processor: Callable[..., Any],
        inputs: list[str] | str | None,
        params: dict[str, Any] | None = None,
        input_data: Any = None,
        save_method: Callable[[Any, str | Path], None] | None = None,
        save_path: str | Path | None = None,
    ) -> None:

        if name in self.steps:
            raise ValueError(f"Step '{name}' already exists in the pipeline.")

        if isinstance(inputs, str):
            inputs = [inputs]

        if inputs is None:
            inputs = []

        if not inputs and input_data is None:
            raise ValueError(f"Step '{name}' must have either inputs or input data.")

        if params is None:
            params = {}

        self.steps[name] = Step(
            name=name,
            processor=processor,
            inputs=inputs,
            params=params,
            input_data=input_data,
            save_method=save_method,
            save_path=save_path,
        )

        self.sorter.add(name, *inputs)

    def run(self) -> None:
        execution_order = list(self.sorter.static_order())

        for step_name in execution_order:
            step = self.steps[step_name]
            inputs = step.inputs

            input_values = [self.steps[input_name].data for input_name in inputs]
            if not input_values:
                input_values = [step.input_data]

            output = step.processor(*input_values, **step.params)
            step.data = output
            if step.save_method is not None and step.save_path is not None:
                step.save_method(output, step.save_path)

    def get_output(self, name: str) -> Any:
        step = self.steps.get(name)
        if step is None:
            raise ValueError(f"Step '{name}' does not exist in the pipeline.")
        if step.data is None:
            raise ValueError(f"Step '{name}' has not been executed yet.")
        return step.data
