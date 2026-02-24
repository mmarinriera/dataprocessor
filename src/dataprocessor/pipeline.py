import json
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import Future
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from dataclasses import dataclass
from dataclasses import field
from graphlib import TopologicalSorter
from pathlib import Path
from typing import Any
from typing import Literal

from dataprocessor.logger import get_logger
from dataprocessor.utils import ValidationError
from dataprocessor.utils import get_func_arg_type_annotations
from dataprocessor.utils import get_func_required_args
from dataprocessor.utils import get_func_return_type_annotation

logger = get_logger()


@dataclass
class Step:
    name: str
    processor: Callable[..., Any]
    inputs: list[str] = field(default_factory=list)
    params: dict[str, Any] = field(default_factory=dict)
    input_data: Any = None
    input_path: str | Path | None = None
    output_path: str | Path | None = None
    load_method: Callable[[str | Path], Any] | None = None
    save_method: Callable[[Any, str | Path], None] | None = None
    _data: Any | None = field(init=False, default=None)

    @property
    def data(self) -> Any:
        if self._data is None:
            raise AttributeError(f"Step '{self.name}': Attempted data retrieval before solving.")
        return self._data

    @data.setter
    def data(self, data: Any) -> None:
        self._data = data


class Pipeline:
    """A simple data processing pipeline that allows you to define steps with dependencies and execute them in the correct order."""

    def __init__(self, force_run: bool = True, metadata_path: str | Path | None = None) -> None:
        self.steps: dict[str, Step] = {}
        self.sorter: TopologicalSorter[str] = TopologicalSorter()
        self.force_run = force_run

        self.metadata_path = Path(metadata_path).with_suffix(".json") if metadata_path is not None else None
        self.track_metadata = metadata_path is not None
        self.metadata: dict[str, dict[str, Any]] = {"steps": {}}
        self.tracked_metadata: dict[str, dict[str, Any]] | None = None
        if self.track_metadata and self.metadata_path is not None:
            if self.metadata_path.exists():
                with open(self.metadata_path) as f:
                    self.tracked_metadata = json.load(f)

    def _update_metadata(self, step: Step) -> None:
        self.metadata["steps"][step.name] = {
            "processor": getattr(step.processor, "__name__", "no_processor_name"),
            "inputs": step.inputs,
            "params": step.params,
            "input_path": str(step.input_path) if step.input_path else None,
            "output_path": str(step.output_path) if step.output_path else None,
        }

    def add_step(
        self,
        name: str,
        processor: Callable[..., Any],
        inputs: list[str] | str | None = None,
        params: dict[str, Any] | None = None,
        input_data: Any = None,
        input_path: str | Path | None = None,
        output_path: str | Path | None = None,
        load_method: Callable[[str | Path], Any] | None = None,
        save_method: Callable[[Any, str | Path], None] | None = None,
    ) -> None:

        if name in self.steps:
            raise ValueError(f"Step '{name}' already exists in the pipeline.")

        if isinstance(inputs, str):
            inputs = [inputs]

        if inputs is None:
            inputs = []

        if not inputs and input_data is None and input_path is None:
            raise ValueError(f"Step '{name}': must have either inputs, input data, or an input path.")

        if input_path is not None and load_method is None:
            raise ValueError(f"Step '{name}': a load_method must be provided if input_path is specified.")

        if params is None:
            params = {}

        step = Step(
            name=name,
            processor=processor,
            inputs=inputs,
            params=params,
            input_data=input_data,
            input_path=input_path,
            load_method=load_method,
            save_method=save_method,
            output_path=output_path,
        )

        self.steps[name] = step
        self.sorter.add(name, *inputs)

        if self.track_metadata:
            self._update_metadata(step)

    def _autoload_allowed(self, step: Step) -> bool:
        if step.load_method is None or step.output_path is None:
            logger.debug(f"Step '{step.name}': Autoload not allowed because load_method or output_path is not defined.")
            return False

        if not Path(step.output_path).exists():
            logger.debug(f"Step '{step.name}': Output file '{step.output_path}' does not exist.")
            return False

        # Check that the output file is newer than all input files
        output_mtime = Path(step.output_path).stat().st_mtime

        if step.input_path is not None:
            input_mtime = Path(step.input_path).stat().st_mtime
            if input_mtime > output_mtime:
                logger.debug(f"step input file '{step.input_path}' is newer than output file '{step.output_path}'.")
                return False

        for input_name in step.inputs:
            input_step = self.steps[input_name]
            if input_step.output_path is None or not Path(input_step.output_path).exists():
                logger.debug(f"input file not found. {input_step.output_path}")
                return False

            input_mtime = Path(input_step.output_path).stat().st_mtime
            if input_mtime > output_mtime:
                logger.debug(
                    f"file from input step '{input_step.output_path}' is newer than output file '{step.output_path}'."
                )
                return False

        if not self.track_metadata:
            return True

        if self.tracked_metadata is None:
            logger.debug(f"Step '{step.name}': No tracked metadata found, cannot validate autoload.")
            return False

        # Check that tracked step metadata matches current step configuration
        tracked_step_metadata = self.tracked_metadata["steps"].get(step.name)
        step_metadata = self.metadata["steps"].get(step.name)

        if step_metadata != tracked_step_metadata:
            logger.debug(f"Step '{step.name}': Tracked metadata does not match current step configuration.")
            return False

        return True

    def _build_sorter(self) -> TopologicalSorter[str]:
        sorter: TopologicalSorter[str] = TopologicalSorter()
        for step in self.steps.values():
            sorter.add(step.name, *step.inputs)
        return sorter

    def _get_input_values(self, step: Step) -> list[Any]:
        inputs = step.inputs

        if step.input_path is not None:
            logger.info(f"Step '{step.name}': Loading input from file,'{step.input_path}'.")
            return [step.load_method(step.input_path)]  # type: ignore
        if step.input_data is not None:
            logger.info(f"Step '{step.name}': Using provided input data.")
            return [step.input_data]
        logger.info(f"Step '{step.name}': Using provided input steps.")
        return [self.steps[input_name].data for input_name in inputs]

    def run(
        self,
        parallel: bool = False,
        mode: Literal["thread", "process"] = "thread",
        max_workers: int | None = None,
    ) -> None:
        if not parallel:
            execution_order = list(self._build_sorter().static_order())

            for step_name in execution_order:
                step = self.steps[step_name]
                input_values = self._get_input_values(step)

                if not self.force_run:
                    if self._autoload_allowed(step):
                        logger.info(
                            f"Step '{step.name}': Output file '{step.output_path}' found. Loading output from file."
                        )
                        step.data = step.load_method(step.output_path)  # type: ignore
                        continue
                    logger.info(
                        f"Step '{step.name}': Output file '{step.output_path}' not found or outdated. Recomputing step."
                    )

                output = step.processor(*input_values, **step.params)
                step.data = output
                if step.save_method is not None and step.output_path is not None:
                    step.save_method(output, step.output_path)
        else:
            if mode not in {"thread", "process"}:
                raise ValueError(f"Invalid mode '{mode}'. Expected one of: thread, process.")

            sorter = self._build_sorter()
            sorter.prepare()
            executor_cls = ThreadPoolExecutor if mode == "thread" else ProcessPoolExecutor
            in_flight: dict[Future[Any], str] = {}

            with executor_cls(max_workers=max_workers) as executor:
                while sorter.is_active():
                    for step_name in sorter.get_ready():
                        step = self.steps[step_name]
                        input_values = self._get_input_values(step)

                        if not self.force_run:
                            if self._autoload_allowed(step):
                                logger.info(
                                    f"Step '{step.name}': Output file '{step.output_path}' found. Loading output from file."
                                )
                                step.data = step.load_method(step.output_path)  # type: ignore
                                sorter.done(step_name)
                                continue
                            logger.info(
                                f"Step '{step.name}': Output file '{step.output_path}' not found or outdated. Recomputing step."
                            )

                        future = executor.submit(step.processor, *input_values, **step.params)
                        in_flight[future] = step_name

                    if not in_flight:
                        continue

                    done_futures, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                    for future in done_futures:
                        step_name = in_flight.pop(future)
                        step = self.steps[step_name]
                        output = future.result()
                        step.data = output
                        if step.save_method is not None and step.output_path is not None:
                            step.save_method(output, step.output_path)
                        sorter.done(step_name)

        if self.track_metadata and self.metadata_path is not None:
            with open(self.metadata_path, "w") as f:
                json.dump(self.metadata, f, indent=4)
                f.write("\n")

    def get_output(self, name: str) -> Any:
        step = self.steps.get(name)
        if step is None:
            raise ValueError(f"Step '{name}' does not exist in the pipeline.")
        return step.data

    def validate_step_types(self) -> None:
        for step in self.steps.values():
            if step.input_path is not None:
                input_types = [get_func_return_type_annotation(step.load_method)]  # type: ignore
            elif step.input_data is not None:
                # Type validation for input data literals not supported.
                input_types = []
            else:
                input_steps = [self.steps[input_name] for input_name in step.inputs]
                input_types = [get_func_return_type_annotation(input_step.processor) for input_step in input_steps]
            processor_arg_types = get_func_arg_type_annotations(step.processor)
            logger.debug(
                f"Step '{step.name}': Inferred input types: {input_types} // Processor argument types: {processor_arg_types}"
            )
            if not all(t == u for t, u in zip(input_types, processor_arg_types.values())):
                raise ValidationError(f"Step '{step.name}': Input types do not match processor inputs.")

            processor_arg_types = get_func_arg_type_annotations(step.processor)

            # Check that all required processor arguments are provided by either inputs or params
            required_arg_names = get_func_required_args(step.processor)
            n_inputs = len(step.inputs) or 1
            required_param_arg_names = required_arg_names[n_inputs:]
            for arg_name in required_param_arg_names:
                if arg_name not in step.params:
                    raise ValidationError(
                        f"Step '{step.name}': Required parameter '{arg_name}' not provided in params."
                    )

            if not step.params:
                continue

            # Check that all parameters passed match an argument in the processor and that the types match
            for param_name, param_value in step.params.items():
                if param_name not in processor_arg_types:
                    raise ValidationError(
                        f"Step '{step.name}': Parameter '{param_name}' not found in processor arguments."
                    )
                expected_type = processor_arg_types[param_name]
                if not isinstance(param_value, expected_type):
                    raise ValidationError(
                        f"Step '{step.name}': Parameter '{param_name}' expected type {expected_type}, got {type(param_value)}."
                    )
