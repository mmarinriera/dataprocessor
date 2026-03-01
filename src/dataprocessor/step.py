import itertools
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any

from dataprocessor.logger import get_logger
from dataprocessor.utils import LoadMethod
from dataprocessor.utils import SaveMethod

logger = get_logger()


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

    def load_input_from_file(self) -> Any:
        """
        Load step input data from file specified in ``input_path`` using ``input_load_method``.

        Returns:
            Loaded input data.

        Raises:
            ValueError: If input_path or input_load_method is not defined.

        """
        if self.input_path is None or self.input_load_method is None:
            raise ValueError(
                f"Step '{self.name}': cannot load input from file because input_path or input_load_method is not defined."
            )

        return self.input_load_method(self.input_path)

    def save_output(self) -> None:
        """
        Save data to specified output files.

        Doesn't save data if output_path or save_method is not defined.
        """
        if self.output_path is None or self.save_method is None:
            logger.debug(f"Step '{self.name}': Output not save because output_path or output_method is not set.")
            return

        if not isinstance(self.output_path, tuple):
            self.save_method(self.data, self.output_path)  # type: ignore
            return

        n_output_paths = len(self.output_path)
        save_methods = (
            n_output_paths * [self.save_method] if not isinstance(self.save_method, tuple) else list(self.save_method)
        )
        for d, o_path, s_method in zip(self.data, self.output_path, save_methods, strict=True):
            s_method(d, o_path)

    def load_output(self) -> None:
        """
        Load data attribute from specified output files.

        Raises:
            ValueError: If output_path or load_method is not defined.

        """
        if self.load_method is None or self.output_path is None:
            raise ValueError(
                f"Step '{self.name}': cannot load output from file because load_method or output_path is not defined."
            )

        if not isinstance(self.output_path, tuple):
            self.data = self.load_method(self.output_path)  # type: ignore
            return

        n_output_paths = len(self.output_path)
        load_methods = (
            n_output_paths * [self.load_method] if not isinstance(self.load_method, tuple) else list(self.load_method)
        )

        self.data = tuple(l_method(o_path) for o_path, l_method in zip(self.output_path, load_methods, strict=True))
