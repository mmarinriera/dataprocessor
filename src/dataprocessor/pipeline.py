import itertools
import json
import sys
from collections.abc import Callable
from concurrent.futures import Future
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from dataclasses import dataclass
from dataclasses import field
from graphlib import TopologicalSorter
from pathlib import Path
from typing import Any
from typing import Literal
from typing import TypeAlias

from dataprocessor.logger import get_logger
from dataprocessor.utils import ValidationError
from dataprocessor.utils import get_func_arg_type_annotations
from dataprocessor.utils import get_func_arg_types
from dataprocessor.utils import get_func_required_args
from dataprocessor.utils import get_func_return_type_annotation

if sys.version_info >= (3, 11):
    TopologicalSorterStr: TypeAlias = TopologicalSorter[str]
else:
    TopologicalSorterStr: TypeAlias = TopologicalSorter

LoadMethod = Callable[[str | Path], Any]
SaveMethod = Callable[[Any, str | Path], None]


logger = get_logger()


class PipelineExecutionError(RuntimeError):
    """
    Pipeline execution has failed during runtime.

    Attributes:
        failed_steps: Names of steps that raised an exception.
        skipped_steps: Names of steps skipped due to failed dependencies.

    """

    def __init__(self, failed_steps: list[str], skipped_steps: list[str]) -> None:
        self.failed_steps = failed_steps
        self.skipped_steps = skipped_steps
        details = f"Pipeline failed. Failed steps: {failed_steps}"
        if skipped_steps:
            details = f"{details}. Skipped steps: {skipped_steps}"
        super().__init__(details)


@dataclass
class Step:
    """
    Definition of a processing step in the pipeline.

    Attributes:
        name: Name of the step.
        processor: Callable that transforms step input(s) into output.
        inputs: References to other steps' outputs to be passed as inputs to the processor.
            If a step has a single output, it is referenced by the step name.
            If a step has multiple outputs (defined in ``outputs``),
            they are referenced by ``<step_name>.<output_name>``.
            The data from those input steps will be passed as positional arguments to ``processor``.
        params: Extra keyword arguments passed to ``processor``.
        input_data: Literal input value passed as input to processor.
            If ``input_data`` is set, other input sources will be ignored.
        input_path: File path to load input data for step.
            If ``input_path`` is set, `inputs` will be ignored.
            Requires ``load_method`` to be set.
        outputs: Names of outputs to reference in other steps,
            if the processor produces multiple outputs (as a tuple).
            If set to None, it is assumed the processor returns a single output.
        output_path: File path/s where output data is saved.
            Requires ``save_method`` to be set.
        input_load_method: Function used to load input data from ``input_path``.
        load_method: Function/s used to load data cached output specified in ``output_path``.
        save_method: Function/s used to save output data to ``output_path``.

    """

    name: str
    processor: Callable[..., Any]
    inputs: list[str] = field(default_factory=list)
    params: dict[str, Any] = field(default_factory=dict)
    input_data: Any = None
    input_path: str | Path | None = None
    outputs: list[str] | None = None
    output_path: str | Path | tuple[str | Path, ...] | None = None
    input_load_method: LoadMethod | None = None
    load_method: LoadMethod | tuple[LoadMethod, ...] | None = None
    save_method: SaveMethod | tuple[SaveMethod, ...] | None = None
    _data: Any | None = field(init=False, default=None)

    def __hash__(self) -> int:
        return hash(self.name)

    def __post_init__(self) -> None:
        self._validate()

    @property
    def data(self) -> Any:
        """
        Return solved data for the step.

        Returns:
            Any: Output data produced for this step.

        Raises:
            AttributeError: If data is requested before the step has produced output.

        """
        if self._data is None:
            raise AttributeError(f"Step '{self.name}': Attempted data retrieval before solving.")
        return self._data

    @data.setter
    def data(self, data: Any) -> None:
        """
        Store solved data for the step.

        Args:
            data: Output value to attach to the step.

        """
        self._data = data

    def _validate(self) -> None:
        """
        Internal step data validation.

        - Check that theres is at least one input source: ``input_path``, ``input_data`` or ``inputs``.
        - Check that ``input_load_method`` is specified if ``input_path`` is specified.
        - Check that ``load_method`` is specified if ``output_path`` are specified.
        - If ``input_path`` is set, check that the file exists.
        - If there are multiple outputs, check that there is a single load method, or as many as outputs.
        - If there are multiple outputs, check that there is a single save method, or as many as outputs.

        Raises:
            ValueError: If any of the checks fail.

        """
        has_input_source = bool(self.inputs) or self.input_data is not None or self.input_path is not None
        if not has_input_source:
            raise ValueError(f"Step '{self.name}': must have either inputs, input data, or an input path.")

        if self.input_path is not None and self.input_load_method is None:
            raise ValueError(f"Step '{self.name}': an input_load_method must be provided if input_path is specified.")

        if self.output_path is not None and self.load_method is None:
            raise ValueError(f"Step '{self.name}': a load_method must be provided if output_path is specified.")

        if self.input_path is not None and not Path(self.input_path).exists():
            raise ValueError(f"Step '{self.name}': input_path '{self.input_path}' does not exist.")

        n_outputs = len(self.outputs) if self.outputs is not None else 1
        if n_outputs <= 1:
            return

        if isinstance(self.load_method, tuple) and len(self.load_method) not in {1, n_outputs}:
            raise ValueError(
                f"Step '{self.name}': load_method must be a single callable or a tuple with {n_outputs} methods."
            )

        if isinstance(self.save_method, tuple) and len(self.save_method) not in {1, n_outputs}:
            raise ValueError(
                f"Step '{self.name}': save_method must be a single callable or a tuple with {n_outputs} methods."
            )

    def output_files_exist(self) -> bool:
        """
        Checks whether the files specified in ``output_path`` exist.

        Returns:
            True if all files specified in ``output_path`` exist, False otherwise.

        """
        if self.output_path is None:
            return False

        output_paths = list(self.output_path) if isinstance(self.output_path, tuple) else [self.output_path]

        return all(Path(p).exists() for p in output_paths)

    def is_more_recent_than_output_files(self, files: str | Path | list[str | Path] | tuple[str | Path, ...]) -> bool:
        """
        Checks whether the files passed are more recent than the steps output files, if present.

        Args:
            files: File paths to be checked against steps output paths.

        Returns:
            True if any of the input paths is more recent than the output paths

        """
        if self.output_path is None:
            logger.debug(
                f"Step '{self.name}': cannot compare file with output files because output_path is not defined."
            )
            return False

        files = list(files) if isinstance(files, (list, tuple)) else [files]
        file_mtimes = [Path(file).stat().st_mtime for file in files]

        output_paths = list(self.output_path) if isinstance(self.output_path, tuple) else [self.output_path]
        output_mtimes = [Path(output_path).stat().st_mtime for output_path in output_paths]

        if any(i_mtime > o_mtime for i_mtime, o_mtime in itertools.product(file_mtimes, output_mtimes)):
            logger.debug(f"some step input files '{files}' are newer than one of output files '{output_paths}'.")
            return True
        return False

    def save_output(self) -> None:
        """Save data to specified output files."""
        if self.output_path is None or self.save_method is None:
            logger.debug(f"Step '{self.name}': Output not save because output_path or output_method is not set.")
            return

        if not isinstance(self.output_path, tuple):
            self.save_method(self.data, self.output_path)  # type: ignore
            return

        n_output_paths = len(self.output_path)
        save_methods = (
            n_output_paths * [self.save_method] if not isinstance(self.save_method, tuple) else self.save_method
        )
        for d, o_path, s_method in zip(self.data, self.output_path, save_methods, strict=True):
            s_method(d, o_path)

    def load_output(self) -> None:
        """Load data attribute from specified output files."""
        if not isinstance(self.output_path, tuple):
            self.data = self.load_method(self.output_path)  # type: ignore
            return

        n_output_paths = len(self.output_path)
        load_methods = (
            n_output_paths * [self.load_method] if not isinstance(self.load_method, tuple) else list(self.load_method)
        )

        self.data = tuple(l_method(o_path) for o_path, l_method in zip(self.output_path, load_methods, strict=True))  # type: ignore


class Pipeline:
    """
    A simple data processing pipeline with dependency-aware execution.

    Attributes:
        steps: Mapping of step names to configured :class:`Step` definitions.
        force_run: Whether cached step outputs should be ignored and recomputed.
        metadata_path: JSON metadata file path, or ``None`` when metadata tracking is disabled.
        track_metadata: Flag indicating whether step metadata should be tracked and persisted.
        metadata: In-memory metadata snapshot for currently configured steps.
        tracked_metadata: Metadata loaded from disk for cache-validation comparisons.
        failed_steps: Names of steps that failed during the most recent run.
        skipped_steps: Names of steps skipped due to failed upstream dependencies.
        errors: Mapping of failed step names to their captured exceptions.

    """

    def __init__(self, force_run: bool = True, metadata_path: str | Path | None = None) -> None:
        self.steps: dict[str, Step] = {}
        self.step_output_map: dict[Step, list[str]] = {}
        self.output_step_map: dict[str, Step] = {}

        self.force_run = force_run

        self.metadata_path = Path(metadata_path).with_suffix(".json") if metadata_path is not None else None
        self.track_metadata = metadata_path is not None
        self.metadata: dict[str, dict[str, Any]] = {"steps": {}}
        self.tracked_metadata: dict[str, dict[str, Any]] | None = None
        if self.track_metadata and self.metadata_path is not None:
            if self.metadata_path.exists():
                with open(self.metadata_path) as f:
                    self.tracked_metadata = json.load(f)

        self.failed_steps: set[str] = set()
        self.skipped_steps: set[str] = set()
        self.errors: dict[str, BaseException] = {}

    def _update_metadata(self, step: Step) -> None:
        """
        Update in-memory metadata for a configured step.

        Args:
            step: Step to serialise into the metadata model.

        """
        # Ensure parameters that contain sets are JSON serialisable
        # TODO: Consider adding other non-serialisable types.
        serializable_params = {}
        if step.params:
            for k, v in step.params.items():
                if isinstance(v, set) or isinstance(v, frozenset):
                    v = sorted(list(v))
                serializable_params[k] = v

        if step.output_path is None:
            serialised_output_paths = None
        else:
            serialised_output_paths = (
                [str(p) for p in step.output_path] if isinstance(step.output_path, tuple) else str(step.output_path)
            )

        self.metadata["steps"][step.name] = {
            "processor": getattr(step.processor, "__name__", "no_processor_name"),
            "inputs": step.inputs,
            "outputs": step.outputs or {},
            "params": serializable_params,
            "input_path": str(step.input_path) if step.input_path else None,
            "output_path": serialised_output_paths,
        }

    def add_step(
        self,
        name: str,
        processor: Callable[..., Any],
        inputs: list[str] | str | None = None,
        params: dict[str, Any] | None = None,
        input_data: Any = None,
        input_path: str | Path | None = None,
        outputs: list[str] | None = None,
        output_path: str | Path | tuple[str | Path, ...] | None = None,
        input_load_method: LoadMethod | None = None,
        load_method: LoadMethod | tuple[LoadMethod, ...] | None = None,
        save_method: SaveMethod | tuple[SaveMethod, ...] | None = None,
    ) -> None:
        """
        Register a new processing step in the pipeline.

        Args:
            name: Unique name of the step.
            processor: Callable that transforms step input(s) into output.
            inputs: References to other steps' outputs to be passed as inputs to the processor.
                If a step has a single output, it is referenced by the step name.
                If a step has multiple outputs (defined in ``outputs``),
                they are referenced by ``<step_name>.<output_name>``.
                The data from those input steps will be passed as positional arguments to ``processor``.
            params: Extra keyword arguments passed to ``processor``.
            input_data: Literal input value passed as input to processor.
                If ``input_data`` is set, other input sources will be ignored.
            input_path: File path to load input data for step.
                If ``input_path`` is set, `inputs` will be ignored.
                Requires ``load_method`` to be set.
            outputs: Names of outputs to reference in other steps,
                if the processor produces multiple outputs (as a tuple).
                If set to None, it is assumed the processor returns a single output.
            output_path: File path/s where output data is saved.
                Requires ``save_method`` to be set.
            input_load_method: Function used to load input data from ``input_path``.
            load_method: Function/s used to load data cached output specified in ``output_path``.
            save_method: Function/s used to save output data to ``output_path``.

        Raises:
            ValueError: If step name already exists or required input configuration is missing.

        """
        if name in self.steps:
            raise ValueError(f"Step '{name}' already exists in the pipeline.")

        if isinstance(inputs, str):
            inputs = [inputs]

        if inputs is None:
            inputs = []

        if params is None:
            params = {}

        step = Step(
            name=name,
            processor=processor,
            inputs=inputs,
            params=params,
            input_data=input_data,
            input_path=input_path,
            outputs=outputs,
            input_load_method=input_load_method,
            load_method=load_method,
            save_method=save_method,
            output_path=output_path,
        )

        self.steps[name] = step

        if outputs:
            step_output_refs: list[str] = []
            for output in outputs:
                output_ref = f"{name}.{output}"
                if output_ref in self.steps:
                    raise ValueError(f"Step '{name}': output reference '{output_ref}' matches a step in the pipeline.")
                if output_ref in itertools.chain(self.output_step_map, step_output_refs):
                    raise ValueError(f"Step '{name}': output reference '{output_ref}' already exists in the pipeline.")
                step_output_refs.append(output_ref)
                self.output_step_map[output_ref] = step
            self.step_output_map[step] = step_output_refs

        if self.track_metadata:
            self._update_metadata(step)

    def _get_step_name_from_input_reference(self, reference: str) -> str:
        """Parses a reference to a step input, and returns the name of the corresponding step."""
        if reference in self.steps:
            return reference

        if reference in self.output_step_map:
            return self.output_step_map[reference].name

        raise ValueError(f"Input reference not found: {reference}")

    def _autoload_allowed(self, step: Step) -> bool:
        """
        Check whether a step output can be loaded from file, thus skipping processor execution.

        The criteria for allowing auto-loading:
            - The output files from input steps, or the input file from current step need
                to be older than the output file from the current step.
            - If `Pipeline.track_metadata` is enabled, current step configuration (e.g. passed `param` values)
                need to match the tracked metadata from the previous run.

        Args:
            step: Step being evaluated for auto-loading.

        Returns:
            True if output auto-loading is allowed, False otherwise.

        """
        if step.load_method is None or step.output_path is None:
            logger.debug(f"Step '{step.name}': Autoload not allowed because load_method or output_path is not defined.")
            return False

        if not step.output_files_exist():
            logger.debug(f"Step '{step.name}': Output file '{step.output_path}' does not exist.")
            return False

        # Check that the output file is newer than all input files
        if step.input_path is not None and step.is_more_recent_than_output_files(step.input_path):
            logger.debug(f"Step '{step.name}': Input file is more recent than output files.")
            return False

        for input_ref in step.inputs:
            input_step = self.steps[self._get_step_name_from_input_reference(input_ref)]
            if input_step.output_path is None or not input_step.output_files_exist():
                logger.debug(f"input files not found. {input_step.output_path}")
                return False
            if step.is_more_recent_than_output_files(input_step.output_path):
                logger.debug(f"Step '{step.name}': One or more input step files are more recent than output files.")
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

    def _build_sorter(self) -> TopologicalSorterStr:
        """
        Build a topological sorter for current step dependencies.

        Returns:
            Sorter prepared with all pipeline step edges.

        """
        sorter: TopologicalSorterStr = TopologicalSorterStr()
        for step in self.steps.values():
            try:
                step_inputs = [self._get_step_name_from_input_reference(r) for r in step.inputs]
            except ValueError:
                raise ValueError(f"Step '{step.name}': Step inputs not found while building DAG sorter.")
            sorter.add(step.name, *step_inputs)
        return sorter

    def _failed_or_skipped_inputs(self, step: Step) -> bool:
        """
        Determine whether any upstream dependency has failed or been skipped.

        Args:
            step: Step whose dependencies are checked.

        Returns:
            True when execution should be skipped due to failed or skipped inputs.

        """
        blocked_inputs = [
            input_name for input_name in step.inputs if input_name in self.failed_steps | self.skipped_steps
        ]
        if blocked_inputs:
            logger.error(f"Step '{step.name}': Skipped because dependencies failed: {blocked_inputs}.")
            self.skipped_steps.add(step.name)
            return True

        return False

    def _resolve_input_reference(self, input_reference: str) -> Any:
        """
        Parses a reference to a step input, and returns the correct data object.

        If the input reference matches a step name in the pipeline, `step.data` is returned.

        Otherwise, if the input reference matches an output reference (defined in step.outputs),
            ``step.data[reference_index]`` is returned, where reference_index is the index of the reference in
            ``step.outputs``.

        Args:
            input_reference: Input reference to be resolved.

        Returns:
            Data object.

        Raises:
            ValueError: If input_reference doesn't match any step name, or any output reference registered in the pipeline.

        """
        if input_reference in self.steps:
            return self.steps[input_reference].data

        if input_reference in self.output_step_map:
            step = self.output_step_map[input_reference]
            output_refs = self.step_output_map.get(step)
            if output_refs is None:
                raise ValueError(
                    f"Step '{step.name}': input reference '{input_reference}' is missing in step_output_map."
                )

            try:
                output_index = output_refs.index(input_reference)
            except ValueError as exc:
                raise ValueError(
                    f"Step '{step.name}': input reference '{input_reference}' was not registered for this step."
                ) from exc

            return step.data[output_index]

        raise ValueError(f"Input reference '{input_reference}' not found in pipeline steps or output references.")

    def _get_input_values(self, step: Step) -> list[Any]:
        """
        Resolve positional input values for a step.

        Args:
            step: Step to resolve inputs for.

        Returns:
            Positional argument values to pass to the processor.

        """
        if step.input_path is not None:
            logger.info(f"Step '{step.name}': Loading input from file,'{step.input_path}'.")
            return [step.input_load_method(step.input_path)]  # type: ignore
        if step.input_data is not None:
            logger.info(f"Step '{step.name}': Using provided input data.")
            return [step.input_data]
        logger.info(f"Step '{step.name}': Using provided input steps.")
        return [self._resolve_input_reference(input_ref) for input_ref in step.inputs]

    def _attempt_output_load(self, step: Step) -> bool:
        """
        Try loading a cached output value for a step.

        Args:
            step: Step to load from cache when possible.

        Returns:
            True if cached output was loaded, False if recomputation is required.

        """
        if self.force_run:
            return False
        if self._autoload_allowed(step):
            logger.info(f"Step '{step.name}': Output file '{step.output_path}' found. Loading output from file.")
            step.load_output()
            return True
        logger.info(f"Step '{step.name}': Output file '{step.output_path}' not found or outdated. Recomputing step.")
        return False

    def _run_serial(self, fail_fast: bool) -> None:
        """
        Execute pipeline steps serially in dependency order.

        Args:
            fail_fast: If True, stop execution immediately on first step failure.

        Raises:
            RuntimeError: If ``fail_fast`` is True and a step raises an exception.

        """
        execution_order = list(self._build_sorter().static_order())

        for step_name in execution_order:
            step = self.steps[step_name]

            if self._failed_or_skipped_inputs(step):
                continue

            input_values = self._get_input_values(step)
            if self._attempt_output_load(step):
                continue

            try:
                output = step.processor(*input_values, **step.params)
                step.data = output
                step.save_output()
            except BaseException as exc:
                self.failed_steps.add(step_name)
                self.errors[step_name] = exc
                logger.exception(f"Step '{step.name}' failed.")
                if fail_fast:
                    raise RuntimeError(f"Step '{step.name}': Aborting pipeline execution due to step failure.") from exc

    def _run_parallel(
        self, mode: Literal["thread", "process"], max_workers: int | None, fail_fast: bool = False
    ) -> None:
        """
        Execute pipeline steps in parallel while respecting dependencies.

        Args:
            mode: Parallel backend mode, either ``"thread"`` or ``"process"``.
            max_workers: Maximum number of worker threads/processes.
            fail_fast: If True, cancel pending work and abort after first failure.

        Raises:
            ValueError: If ``mode`` is not a supported value.
            RuntimeError: If ``fail_fast`` is True and a step raises an exception.

        """
        if mode not in {"thread", "process"}:
            raise ValueError(f"Invalid parallel run mode '{mode}'. Expected one of: thread, process.")

        sorter = self._build_sorter()
        sorter.prepare()
        executor_cls = ThreadPoolExecutor if mode == "thread" else ProcessPoolExecutor
        running: dict[Future[Any], str] = {}

        with executor_cls(max_workers=max_workers) as executor:
            while sorter.is_active():
                for step_name in sorter.get_ready():
                    step = self.steps[step_name]
                    if self._failed_or_skipped_inputs(step):
                        sorter.done(step_name)
                        continue

                    input_values = self._get_input_values(step)
                    if self._attempt_output_load(step):
                        sorter.done(step_name)
                        continue

                    future = executor.submit(step.processor, *input_values, **step.params)
                    running[future] = step_name

                logger.debug(f"Steps running in parallel: {list(running.values())}")
                if not running:
                    continue

                for future in as_completed(running):
                    step_name = running.pop(future)
                    step = self.steps[step_name]
                    try:
                        output = future.result()
                        step.data = output
                        step.save_output()
                    except BaseException as exc:
                        self.failed_steps.add(step_name)
                        self.errors[step_name] = exc
                        logger.exception(f"Step '{step.name}': failed to run.")
                        sorter.done(step_name)
                        if fail_fast:
                            for pending in running:
                                pending.cancel()
                            raise RuntimeError(
                                f"Step '{step.name}': Aborting pipeline execution due to step failure."
                            ) from exc
                        continue
                    sorter.done(step_name)

    def run(
        self,
        parallel: Literal["thread", "process"] | None = None,
        max_workers: int | None = None,
        fail_fast: bool = True,
    ) -> None:
        """
        Run the pipeline.

        Args:
            parallel: Optional parallel mode. If ``None``, runs serially.
            max_workers: Maximum workers for parallel execution.
            fail_fast: If True, abort on first failed step.

        Raises:
            PipelineExecutionError: If one or more steps fail during execution.

        """
        if parallel is None:
            self._run_serial(fail_fast=fail_fast)
        else:
            self._run_parallel(mode=parallel, max_workers=max_workers, fail_fast=fail_fast)

        if self.errors:
            raise PipelineExecutionError(failed_steps=sorted(self.errors), skipped_steps=sorted(self.skipped_steps))

        if self.track_metadata and self.metadata_path is not None:
            with open(self.metadata_path, "w") as f:
                json.dump(self.metadata, f, indent=4)
                f.write("\n")

    def get_output(self, name: str) -> Any:
        """
        Retrieve output data for a named step.

        Args:
            name: Step name whose output should be returned.

        Returns:
            Output value produced by the named step.

        Raises:
            ValueError: If ``name`` does not exist in the pipeline.
            AttributeError: If the step has not produced output yet.

        """
        step = self.steps.get(name)
        if step is None:
            raise ValueError(f"Step '{name}' does not exist in the pipeline.")
        return step.data

    def validate_step_types(self) -> None:
        """
        Validate processor input and parameter types for all configured steps.

        - Check that processor argument type annotations match return type annotations of the
            input step processors.
        - Check that all required processor arguments are provided by step ``params``.
        - Check that all passed `params` match a processor argument, and their types match.

        Note: Type checking for literal values (i.e. values passed to ``Step.input_data``) not yet supported.

        Note: When validating parameters of iterable type, type checking of containing elements is not yet supported.

        Raises:
            ValidationError: If input types or parameter definitions do not match processor signatures.

        """
        for step in self.steps.values():
            if step.outputs:
                raise NotImplementedError(
                    f"Step '{step.name}': Type validation for multiple outputs not yet supported."
                )

            if step.input_path is not None:
                input_types = [get_func_return_type_annotation(step.input_load_method)]  # type: ignore
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

            processor_arg_types = get_func_arg_types(step.processor)

            # Check that all parameters passed match an argument in the processor and that the types match
            for param_name, param_value in step.params.items():
                if param_name not in processor_arg_types:
                    raise ValidationError(
                        f"Step '{step.name}': Parameter '{param_name}' not found in processor arguments."
                    )
                expected_type = processor_arg_types[param_name]
                logger.debug(
                    f"Step '{step.name}': Validating parameter '{param_name}' with value '{param_value}' against expected type '{expected_type}'."
                )
                if not isinstance(param_value, expected_type):
                    raise ValidationError(
                        f"Step '{step.name}': Parameter '{param_name}' expected type {expected_type}, got {type(param_value)}."
                    )
