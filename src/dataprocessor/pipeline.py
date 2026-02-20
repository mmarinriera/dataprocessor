from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from graphlib import TopologicalSorter
from typing import Any


@dataclass
class Step:
    name: str
    processor: Callable[..., Any]
    inputs: list[str] = field(default_factory=list)
    params: dict[str, Any] = field(default_factory=dict)
    input_data: Any = None
    data: Any | None = field(init=False, default=None)


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
        )

        self.sorter.add(name, *inputs)

    def run(self) -> None:
        # Get the execution order of the steps
        execution_order = list(self.sorter.static_order())
        for step_name in execution_order:
            print(f"step name {step_name}")
            step = self.steps[step_name]
            processor = step.processor
            inputs = step.inputs
            params = step.params
            # Get the input values for the current step
            input_values = [self.steps[input_name].data for input_name in inputs]
            if not input_values:
                input_values = [step.input_data]
            print(f"input values {input_values}")
            # Execute the processor function with the input values and parameters
            output = processor(*input_values, **params)
            step.data = output

    def get_output(self, name: str) -> Any:
        step = self.steps.get(name)
        if step is None:
            raise ValueError(f"Step '{name}' does not exist in the pipeline.")
        if step.data is None:
            raise ValueError(f"Step '{name}' has not been executed yet.")
        return step.data
