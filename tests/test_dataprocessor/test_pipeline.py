import csv
import json
import os
import threading
from pathlib import Path
from typing import Any

import pytest
from pytest_lazy_fixtures import lf

from dataprocessor.pipeline import Pipeline
from dataprocessor.pipeline import PipelineExecutionError
from dataprocessor.pipeline import Step
from dataprocessor.utils import ValidationError

# Processors used in tests


def _return_same(x: list[int]) -> list[int]:
    return x


def _load_sequence_dummy(_: str | Path) -> list[int]:
    return [1, 2, 3, 4]


def _load_sequence_csv(filename: str | Path) -> list[int]:
    """Loads a list of integers from a CSV file."""
    with open(filename, newline="") as f:
        reader = csv.reader(f)
        return [int(x) for x in next(reader)]


def _load_sequence_csv_colon(filename: str | Path) -> list[int]:
    """Loads a list of integers from a CSV file with colon separator."""
    with open(filename, newline="") as f:
        reader = csv.reader(f, delimiter=":")
        return [int(x) for x in next(reader)]


def _save_sequence_csv(input: list[int], filename: str | Path) -> None:
    """Saves the input list as a CSV file."""
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(input)


def _save_sequence_csv_colon(input: list[int], filename: str | Path) -> None:
    """Saves the input list as a CSV file with colon separator."""
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f, delimiter=":")
        writer.writerow(input)


def _scale(x: list[int], factor: int) -> list[int]:
    return [i * factor for i in x]


def _filter_from_values(x: list[int], values: list[int]) -> list[int]:
    return [i for i in x if i not in values]


def _processor_str_sequence(x: list[str]) -> list[str]:
    return x


def _split_odd_even(x: list[int]) -> tuple[list[int], list[int]]:
    odd = [value for value in x if value % 2 != 0]
    even = [value for value in x if value % 2 == 0]
    return odd, even


###########################


def test_step_init() -> None:
    step = Step(
        name="test",
        processor=lambda x: x,
        input_data=5,
    )
    step.data = 10


def test_step_access_data_before_solve() -> None:
    step = Step(
        name="test",
        processor=lambda x: x,
        input_data=5,
    )
    with pytest.raises(AttributeError, match="Step 'test': Attempted data retrieval before solving."):
        step.data


def test_step_validate_requires_input_source() -> None:
    with pytest.raises(ValueError, match="Step 'step': must have either inputs, input data, or an input path."):
        Step(name="step", processor=lambda x: x)


def test_step_validate_requires_input_load_method_when_input_path_set(tmp_path: Path) -> None:
    input_path = tmp_path / "input.csv"
    input_path.write_text("1,2,3\n")

    with pytest.raises(
        ValueError,
        match="Step 'step': an input_load_method must be provided if input_path is specified.",
    ):
        Step(name="step", processor=_return_same, input_path=input_path)


def test_step_validate_requires_load_method_when_output_path_set() -> None:
    with pytest.raises(
        ValueError,
        match="Step 'step': a load_method must be provided if output_path is specified.",
    ):
        Step(name="step", processor=_return_same, input_data=[1, 2, 3], output_path="output.csv")


def test_step_validate_rejects_missing_input_file(tmp_path: Path) -> None:
    missing_input_path = tmp_path / "missing.csv"

    with pytest.raises(ValueError, match=f"Step 'step': input_path '{missing_input_path}' does not exist."):
        Step(name="step", processor=_return_same, input_path=missing_input_path, input_load_method=_load_sequence_dummy)


def test_step_validate_accepts_existing_input_file(tmp_path: Path) -> None:
    input_path = tmp_path / "input.csv"
    input_path.write_text("1,2,3\n")

    step = Step(name="step", processor=_return_same, input_path=input_path, input_load_method=_load_sequence_dummy)

    assert step.input_path == input_path


def test_step_validate_multi_output_rejects_invalid_load_method_tuple_length() -> None:
    with pytest.raises(
        ValueError,
        match="Step 'step': load_method must be a single callable or a tuple with 2 methods.",
    ):
        Step(
            name="step",
            processor=_return_same,
            input_data=[1, 2, 3],
            outputs=["left", "right"],
            load_method=(_load_sequence_dummy, _load_sequence_dummy, _load_sequence_dummy),
        )


def test_step_validate_multi_output_rejects_invalid_save_method_tuple_length() -> None:
    with pytest.raises(
        ValueError,
        match="Step 'step': save_method must be a single callable or a tuple with 2 methods.",
    ):
        Step(
            name="step",
            processor=_return_same,
            input_data=[1, 2, 3],
            outputs=["left", "right"],
            load_method=_load_sequence_dummy,
            save_method=(_save_sequence_csv, _save_sequence_csv, _save_sequence_csv),
        )


def test_step_validate_multi_output_accepts_single_load_and_save_method() -> None:
    step = Step(
        name="step",
        processor=_return_same,
        input_data=[1, 2, 3],
        outputs=["left", "right"],
        load_method=_load_sequence_dummy,
        save_method=_save_sequence_csv,
    )

    assert step.outputs == ["left", "right"]


def test_step_validate_multi_output_accepts_matching_tuple_lengths() -> None:
    step = Step(
        name="step",
        processor=_return_same,
        input_data=[1, 2, 3],
        outputs=["left", "right"],
        load_method=(_load_sequence_dummy, _load_sequence_dummy),
        save_method=(_save_sequence_csv, _save_sequence_csv),
    )

    assert step.outputs == ["left", "right"]


def _touch_with_mtime(path: Path, mtime: float) -> None:
    path.touch()
    os.utime(path, (mtime, mtime))


def test_step_is_more_recent_than_output_files_without_output_path(tmp_path: Path) -> None:
    input_file = tmp_path / "input.txt"
    input_file.touch()
    step = Step(name="step", processor=_return_same, inputs=["input"], load_method=_load_sequence_dummy)

    assert not step.is_more_recent_than_output_files(input_file)


@pytest.mark.parametrize(
    "input_mtime, output_mtime, result",
    [(1_000.0, 1_100.0, False), (1_100.0, 1_000.0, True)],
)
def test_step_is_more_recent_than_output_files(
    input_mtime: float, output_mtime: float, result: bool, tmp_path: Path
) -> None:
    input_file = tmp_path / "input.txt"
    output_file = tmp_path / "output.txt"

    _touch_with_mtime(input_file, input_mtime)
    _touch_with_mtime(output_file, output_mtime)

    step = Step(
        name="step",
        processor=_return_same,
        inputs=["input"],
        output_path=output_file,
        load_method=_load_sequence_dummy,
    )

    assert step.is_more_recent_than_output_files(input_file) == result


@pytest.mark.parametrize(
    "input_1_mtime, input_2_mtime, output_1_mtime, output_2_mtime, result",
    [
        (1_000.0, 1_000.0, 1_100.0, 1_100.0, False),
        (1_000.0, 1_200.0, 1_100.0, 1_100.0, True),
        (1_000.0, 1_200.0, 1_100.0, 1_300.0, True),
    ],
)
def test_step_is_more_recent_than_output_files_multiple_inputs_outputs(
    input_1_mtime: float,
    input_2_mtime: float,
    output_1_mtime: float,
    output_2_mtime: float,
    result: bool,
    tmp_path: Path,
) -> None:
    input_1 = tmp_path / "input_1.txt"
    input_2 = tmp_path / "input_2.txt"
    output_1 = tmp_path / "output_1.txt"
    output_2 = tmp_path / "output_2.txt"

    _touch_with_mtime(input_1, input_1_mtime)
    _touch_with_mtime(input_2, input_2_mtime)
    _touch_with_mtime(output_1, output_1_mtime)
    _touch_with_mtime(output_2, output_2_mtime)

    step = Step(
        name="step",
        processor=_return_same,
        inputs=["input"],
        outputs=["output_1", "output_2"],
        output_path=(output_1, output_2),
        load_method=_load_sequence_dummy,
    )

    assert step.is_more_recent_than_output_files([input_1, input_2]) == result


def test_step_save_output_multiple_save_methods(tmp_path: Path, subtests: pytest.Subtests) -> None:
    output_path_1 = tmp_path / "output_1.csv"
    output_path_2 = tmp_path / "output_2.csv"

    step = Step(
        name="step",
        processor=_return_same,
        inputs=["input"],
        outputs=["odd", "even"],
        output_path=(output_path_1, output_path_2),
        load_method=_load_sequence_dummy,
        save_method=_save_sequence_csv,
    )

    step.data = ([1, 3], [2, 4])
    step.save_output()

    with subtests.test("Check output files with one save method"):
        assert output_path_1.read_text() == "1,3\n"
        assert output_path_2.read_text() == "2,4\n"

    step = Step(
        name="step",
        processor=_return_same,
        inputs=["input"],
        outputs=["odd", "even"],
        output_path=(output_path_1, output_path_2),
        load_method=_load_sequence_dummy,
        save_method=(_save_sequence_csv, _save_sequence_csv_colon),
    )

    step.data = ([1, 3], [2, 4])
    step.save_output()
    with subtests.test("Check output files created with multiple save methods"):
        assert output_path_1.read_text() == "1,3\n"
        assert output_path_2.read_text() == "2:4\n"


def test_step_load_output_multiple_load_methods(tmp_path: Path, subtests: pytest.Subtests) -> None:
    output_path_1 = tmp_path / "output_1.csv"
    output_path_1.write_text("1,3\n")
    output_path_2 = tmp_path / "output_2.csv"
    output_path_2.write_text("2,4\n")

    step = Step(
        name="step",
        processor=_return_same,
        inputs=["input"],
        outputs=["odd", "even"],
        output_path=(output_path_1, output_path_2),
        load_method=_load_sequence_csv,
        save_method=_save_sequence_csv,
    )

    step.load_output()

    with subtests.test("Check data loaded with one load method"):
        assert step.data == ([1, 3], [2, 4])

    output_path_2.write_text("2:4\n")
    step = Step(
        name="step",
        processor=_return_same,
        inputs=["input"],
        outputs=["odd", "even"],
        output_path=(output_path_1, output_path_2),
        load_method=(_load_sequence_csv, _load_sequence_csv_colon),
        save_method=_save_sequence_csv,
    )

    step.load_output()
    with subtests.test("Check data loaded with multiple load methods"):
        assert step.data == ([1, 3], [2, 4])


@pytest.mark.parametrize(
    "track_metadata, tracked_metadata_file",
    [
        (False, True),
        (True, True),
        (True, False),
    ],
)
def test_pipeline_init(track_metadata: bool, tracked_metadata_file: bool, tmp_path: Path) -> None:
    metadata_path = tmp_path / "metadata.json"
    target_tracked_metadata = (
        {"steps": {"step_1": {"name": "step_1"}}} if track_metadata and tracked_metadata_file else None
    )

    if tracked_metadata_file:
        with open(metadata_path, "w") as f:
            json.dump(target_tracked_metadata, f)

    pipeline = Pipeline(metadata_path=metadata_path if track_metadata else None)

    assert pipeline.tracked_metadata == target_tracked_metadata


def test_pipeline_add_step(subtests: pytest.Subtests) -> None:

    def some_processor(x: list[int]) -> list[int]:
        return x

    pipeline = Pipeline(metadata_path="dummy_path.json")
    step_data_0: dict[str, Any] = {
        "name": "step_0",
        "processor": some_processor,
        "params": {"some_param": 42},
        "input_data": [1, 2, 3],
    }
    pipeline.add_step(**step_data_0)
    with subtests.test("Step in pipeline dict"):
        assert "step_0" in pipeline.steps

    with subtests.test("Step constructed correctly"):
        assert pipeline.steps["step_0"] == Step(**step_data_0)

    step_data_1: dict[str, Any] = {
        "name": "step_1",
        "processor": lambda x: x,
        "inputs": "step_0",
        "output_path": "/some/output/path",
        "load_method": lambda x: x,
    }
    pipeline.add_step(**step_data_1)

    with subtests.test("Step single input converted to list."):
        assert pipeline.steps["step_1"].inputs == ["step_0"]

    with subtests.test("Step Params dict is empty if not provided"):
        assert pipeline.steps["step_1"].params == {}

    step_data_2: dict[str, Any] = {
        "name": "step_2",
        "processor": lambda x: x,
        "inputs": ["step_0", "step_1"],
        "output_path": "/some/output/path",
        "load_method": lambda x: x,
    }
    pipeline.add_step(**step_data_2)
    with subtests.test("Step multiple inputs remain as list."):
        assert pipeline.steps["step_2"].inputs == ["step_0", "step_1"]

    with subtests.test("Check pipeline metadata"):
        target_metadata = {
            "steps": {
                "step_0": {
                    "processor": "some_processor",
                    "inputs": [],
                    "outputs": {},
                    "params": {"some_param": 42},
                    "input_path": None,
                    "output_path": None,
                },
                "step_1": {
                    "processor": "<lambda>",
                    "params": {},
                    "inputs": ["step_0"],
                    "outputs": {},
                    "input_path": None,
                    "output_path": "/some/output/path",
                },
                "step_2": {
                    "processor": "<lambda>",
                    "params": {},
                    "inputs": ["step_0", "step_1"],
                    "outputs": {},
                    "input_path": None,
                    "output_path": "/some/output/path",
                },
            }
        }
        assert pipeline.metadata == target_metadata

    with subtests.test("Duplicated step name raises error"):
        with pytest.raises(ValueError, match="Step 'step_0' already exists in the pipeline."):
            pipeline.add_step(**step_data_0)

    step_data_no_inputs: dict[str, Any] = {
        "name": "step_no_inputs",
        "processor": lambda x: x,
        "params": {"some_param": 42},
    }
    with (
        subtests.test("Step without input_data"),
        pytest.raises(
            ValueError, match="Step 'step_no_inputs': must have either inputs, input data, or an input path."
        ),
    ):
        pipeline.add_step(**step_data_no_inputs)

    step_no_load_method: dict[str, Any] = {
        "name": "step_no_load_method",
        "processor": lambda x: x,
        "params": {"some_param": 42},
        "input_path": "/some/path",
    }
    with (
        subtests.test("Step with input_path but no input_load_method"),
        pytest.raises(
            ValueError,
            match="Step 'step_no_load_method': an input_load_method must be provided if input_path is specified.",
        ),
    ):
        pipeline.add_step(**step_no_load_method)


@pytest.mark.parametrize(
    "input_path, input_data, inputs, expected_output",
    [
        ("some_path", [3, 2, 1], ["step_0"], [1, 2, 3, 4]),
        ("some_path", None, ["step_0"], [1, 2, 3, 4]),
        ("some_path", None, None, [1, 2, 3, 4]),
        (None, [3, 2, 1], ["step_0"], [3, 2, 1]),
        (None, [3, 2, 1], None, [3, 2, 1]),
        (None, None, ["step_0"], [1, 2, 3]),
    ],
)
def test_pipeline_run_input_precedence(
    input_path: str,
    input_data: list[int],
    inputs: list[str],
    expected_output: list[int],
    tmp_path: Path,
) -> None:

    full_input_path = tmp_path / input_path if input_path is not None else None
    if full_input_path is not None:
        full_input_path.touch()

    step_0: dict[str, Any] = {
        "name": "step_0",
        "processor": _return_same,
        "input_data": [1, 2, 3],
    }

    step_1: dict[str, Any] = {
        "name": "step_1",
        "processor": _return_same,
        "input_path": full_input_path,
        "input_data": input_data,
        "inputs": inputs,
        "input_load_method": _load_sequence_dummy,
    }
    pipeline = Pipeline()
    pipeline.add_step(**step_0)
    pipeline.add_step(**step_1)
    pipeline.run()
    assert pipeline.get_output("step_1") == expected_output


def test_pipeline_run_autoload(tmp_path: Path, subtests: pytest.Subtests, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("DEBUG", logger="DataProcessor")
    input_path = tmp_path / "input.csv"
    with open(input_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([1, 2, 3])

    output_path_0 = tmp_path / "step_0_output.csv"
    output_path_1 = tmp_path / "step_1_output.csv"

    step_0: dict[str, Any] = {
        "name": "step_0",
        "processor": _return_same,
        "input_path": input_path,
        "output_path": output_path_0,
        "input_load_method": _load_sequence_csv,
        "load_method": _load_sequence_csv,
        "save_method": _save_sequence_csv,
    }

    step_1: dict[str, Any] = {
        "name": "step_1",
        "processor": _return_same,
        "inputs": "step_0",
        "load_method": _load_sequence_csv,
        "save_method": _save_sequence_csv,
        "output_path": output_path_1,
    }

    pipeline_0 = Pipeline(force_run=True, metadata_path=tmp_path / "metadata.json")
    pipeline_0.add_step(**step_0)
    pipeline_0.add_step(**step_1)

    log_rerun = [
        f"Step 'step_0': Output file '{output_path_0}' not found or outdated. Recomputing step.",
        f"Step 'step_1': Output file '{output_path_1}' not found or outdated. Recomputing step.",
    ]

    with subtests.test("Pipeline runs for the first time, should execute all steps."):
        pipeline_0.run()
        assert all(msg not in caplog.text for msg in log_rerun)
    caplog.clear()

    pipeline_1 = Pipeline(force_run=True, metadata_path=tmp_path / "metadata.json")
    pipeline_1.add_step(**step_0)
    pipeline_1.add_step(**step_1)
    with subtests.test("Pipeline runs with force_run=True, should execute all steps."):
        pipeline_1.run()
        assert all(msg not in caplog.text for msg in log_rerun)
    caplog.clear()

    pipeline_2 = Pipeline(force_run=False, metadata_path=tmp_path / "metadata.json")
    pipeline_2.add_step(**step_0)
    pipeline_2.add_step(**step_1)

    log_autoload = [
        f"Step 'step_0': Output file '{output_path_0}' found. Loading output from file.",
        f"Step 'step_1': Output file '{output_path_1}' found. Loading output from file.",
    ]
    with subtests.test("Pipeline runs with force_run=False, should auto-load."):
        pipeline_2.run()
        assert all(msg in caplog.text for msg in log_autoload)
    caplog.clear()

    # Overwrite input file to trigger re-run.
    with open(input_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([1, 2, 3])

    pipeline_3 = Pipeline(force_run=False, metadata_path=tmp_path / "metadata.json")
    pipeline_3.add_step(**step_0)
    pipeline_3.add_step(**step_1)
    with subtests.test("Pipeline runs with force_run=False but input file changed, should re-run steps"):
        pipeline_3.run()
        assert all(msg in caplog.text for msg in log_rerun)


def test_pipeline_autoload_metadata(
    tmp_path: Path, subtests: pytest.Subtests, caplog: pytest.LogCaptureFixture
) -> None:
    output_path_0 = tmp_path / "step_0_output.csv"

    step_0: dict[str, Any] = {
        "name": "step_0",
        "processor": _scale,
        "input_data": [1, 2, 3],
        "params": {"factor": 2},
        "output_path": output_path_0,
        "load_method": _load_sequence_csv,
        "save_method": _save_sequence_csv,
    }

    pipeline_0 = Pipeline(force_run=False, metadata_path=tmp_path / "metadata.json")
    pipeline_0.add_step(**step_0)

    log_rerun = f"Step 'step_0': Output file '{output_path_0}' not found or outdated. Recomputing step."

    with subtests.test("Pipeline runs for the first time, should execute all steps."):
        pipeline_0.run()
        assert log_rerun in caplog.text
        assert pipeline_0.get_output("step_0") == [2, 4, 6]
    caplog.clear()

    step_0["params"] = {"factor": 3}
    pipeline_1 = Pipeline(force_run=False, metadata_path=tmp_path / "metadata.json")
    pipeline_1.add_step(**step_0)
    with subtests.test("Pipeline runs with force_run=False but params changed"):
        pipeline_1.run()
        assert log_rerun in caplog.text
        assert pipeline_1.get_output("step_0") == [3, 6, 9]


def test_pipeline_run_parallel_thread_mode() -> None:
    branch_sync = threading.Barrier(2)

    def source(x: int) -> int:
        return x

    def branch(x: int, increment: int) -> int:
        branch_sync.wait(timeout=1)
        return x + increment

    def merge(a: int, b: int) -> int:
        return a + b

    pipeline = Pipeline()
    pipeline.add_step(name="source", processor=source, input_data=1)
    pipeline.add_step(name="left", processor=branch, inputs="source", params={"increment": 1})
    pipeline.add_step(name="right", processor=branch, inputs="source", params={"increment": 2})
    pipeline.add_step(name="merge", processor=merge, inputs=["left", "right"])

    pipeline.run(parallel="thread", max_workers=2)

    assert pipeline.get_output("merge") == 5


def test_pipeline_run_parallel_invalid_mode() -> None:
    pipeline = Pipeline()
    pipeline.add_step(name="step", processor=lambda x: x, input_data=[1, 2, 3])

    with pytest.raises(ValueError, match="Invalid parallel run mode 'invalid'. Expected one of: thread, process."):
        pipeline.run(parallel="invalid")  # type: ignore[arg-type]


def test_pipeline_run_fail_fast_true_default() -> None:
    def source(x: int) -> int:
        return x

    def raises(_: int) -> int:
        raise ValueError("boom")

    pipeline = Pipeline()
    pipeline.add_step(name="source", processor=source, input_data=1)
    pipeline.add_step(name="fails", processor=raises, inputs="source")

    with pytest.raises(RuntimeError, match="Step 'fails': Aborting pipeline execution due to step failure."):
        pipeline.run()


def test_pipeline_run_fail_fast_false_serial() -> None:
    def source(x: int) -> int:
        return x

    def raises(_: int) -> int:
        raise ValueError("boom")

    def add_one(x: int) -> int:
        return x + 1

    pipeline = Pipeline()
    pipeline.add_step(name="source", processor=source, input_data=1)
    pipeline.add_step(name="fails", processor=raises, inputs="source")
    pipeline.add_step(name="ok", processor=add_one, inputs="source")
    pipeline.add_step(name="downstream", processor=add_one, inputs="fails")

    with pytest.raises(
        PipelineExecutionError, match="Failed steps: \\['fails'\\]\\. Skipped steps: \\['downstream'\\]"
    ):
        pipeline.run(fail_fast=False)

    assert pipeline.get_output("ok") == 2
    with pytest.raises(AttributeError, match="Step 'downstream': Attempted data retrieval before solving."):
        pipeline.get_output("downstream")


def test_pipeline_run_fail_fast_false_parallel() -> None:
    def source(x: int) -> int:
        return x

    def raises(_: int) -> int:
        raise ValueError("boom")

    def add_one(x: int) -> int:
        return x + 1

    pipeline = Pipeline()
    pipeline.add_step(name="source", processor=source, input_data=1)
    pipeline.add_step(name="fails", processor=raises, inputs="source")
    pipeline.add_step(name="ok", processor=add_one, inputs="source")
    pipeline.add_step(name="downstream", processor=add_one, inputs="fails")

    with pytest.raises(
        PipelineExecutionError, match="Failed steps: \\['fails'\\]\\. Skipped steps: \\['downstream'\\]"
    ):
        pipeline.run(parallel="thread", max_workers=2, fail_fast=False)

    assert pipeline.get_output("ok") == 2
    with pytest.raises(AttributeError, match="Step 'downstream': Attempted data retrieval before solving."):
        pipeline.get_output("downstream")


def test_pipeline_run_multiple_outputs(tmp_path: Path, subtests: pytest.Subtests) -> None:

    step_0: dict[str, Any] = {
        "name": "step_0",
        "processor": _split_odd_even,
        "input_data": [1, 2, 3, 4, 5, 6],
        "outputs": ["odd", "even"],
        "output_path": (tmp_path / "odd.csv", tmp_path / "even.csv"),
        "load_method": _load_sequence_csv,
        "save_method": _save_sequence_csv,
    }
    step_1: dict[str, Any] = {
        "name": "step_1",
        "processor": _scale,
        "inputs": "step_0.odd",
        "params": {"factor": 10},
        "output_path": tmp_path / "odd_scaled.csv",
        "load_method": _load_sequence_csv,
        "save_method": _save_sequence_csv,
    }
    step_2: dict[str, Any] = {
        "name": "step_2",
        "processor": _scale,
        "inputs": "step_0.even",
        "params": {"factor": 100},
        "output_path": tmp_path / "even_scaled.csv",
        "load_method": _load_sequence_csv,
        "save_method": _save_sequence_csv,
    }
    pipeline = Pipeline()
    pipeline.add_step(**step_0)
    pipeline.add_step(**step_1)
    pipeline.add_step(**step_2)
    pipeline.run()
    with subtests.test("Check outputs of multiple output steps"):
        assert pipeline.get_output("step_1") == ([10, 30, 50])
        assert pipeline.get_output("step_2") == ([200, 400, 600])

    step_wrong_input_ref: dict[str, Any] = {
        "name": "step_wrong_input_ref",
        "processor": _scale,
        "inputs": "step_0.random",
        "params": {"factor": 100},
        "output_path": tmp_path / "even_scaled.csv",
        "load_method": _load_sequence_csv,
        "save_method": _save_sequence_csv,
    }
    pipeline_2 = Pipeline()
    pipeline_2.add_step(**step_0)
    pipeline_2.add_step(**step_wrong_input_ref)
    with subtests.test("Check invalid input reference for multiple output step"):
        with pytest.raises(ValueError, match="Step inputs not found while building DAG sorter."):
            pipeline_2.run()

    step_duplicated_output_names: dict[str, Any] = {
        "name": "step_duplicated_output",
        "processor": _split_odd_even,
        "input_data": [1, 2, 3, 4, 5, 6],
        "outputs": ["odd", "odd"],
        "output_path": (tmp_path / "odd.csv", tmp_path / "even.csv"),
        "load_method": _load_sequence_csv,
        "save_method": _save_sequence_csv,
    }
    pipeline_3 = Pipeline()
    with subtests.test("Check duplicate output names in multiple output step raises error"):
        with pytest.raises(
            ValueError,
            match="Step 'step_duplicated_output': output reference 'step_duplicated_output.odd' already exists in the pipeline.",
        ):
            pipeline_3.add_step(**step_duplicated_output_names)

    step_4: dict[str, Any] = {
        "name": "step_4.odd",
        "processor": _scale,
        "input_data": [1, 2, 3, 4, 5, 6],
        "output_path": tmp_path / "random.csv",
        "load_method": _load_sequence_csv,
        "save_method": _save_sequence_csv,
    }

    step_duplicated_ref: dict[str, Any] = {
        "name": "step_4",
        "processor": _split_odd_even,
        "input_data": [1, 2, 3, 4, 5, 6],
        "outputs": ["odd", "even"],
        "output_path": (tmp_path / "odd.csv", tmp_path / "even.csv"),
        "load_method": _load_sequence_csv,
        "save_method": _save_sequence_csv,
    }
    pipeline_3 = Pipeline()
    pipeline_3.add_step(**step_4)
    with (
        subtests.test("Check duplicate output names in multiple output step raises error"),
        pytest.raises(ValueError, match="Step 'step_4': output reference 'step_4.odd' matches a step in the pipeline."),
    ):
        pipeline_3.add_step(**step_duplicated_ref)


def test_pipeline_get_output(subtests: pytest.Subtests) -> None:
    step_0: dict[str, Any] = {
        "name": "step_0",
        "processor": _return_same,
        "input_data": [1, 2, 3],
    }
    pipeline = Pipeline()
    pipeline.add_step(**step_0)
    with subtests.test("Attempting to get output before running pipeline"):
        with pytest.raises(AttributeError, match="Step 'step_0': Attempted data retrieval before solving."):
            pipeline.get_output("step_0")

    with subtests.test("Attempting to get output from a step not in the pipeline."):
        with pytest.raises(ValueError, match="Step 'step_1' does not exist in the pipeline."):
            pipeline.get_output("step_1")

    pipeline.run()
    with subtests.test("Getting output after running pipeline."):
        assert pipeline.get_output("step_0") == [1, 2, 3]


@pytest.fixture
def existing_input_file(tmp_path: Path) -> Path:
    file_path = tmp_path / "dummy_file.csv"
    file_path.touch()
    return file_path


def test_pipeline_validate_types(existing_input_file: Path) -> None:
    step_0_data: dict[str, Any] = {
        "name": "step_0",
        "processor": _return_same,
        "input_data": [1, 2, 3],
    }
    step_1_data: dict[str, Any] = {
        "name": "step_1",
        "processor": _filter_from_values,
        "inputs": "step_0",
        "params": {"values": [1, 2, 3]},
    }
    pipeline_0 = Pipeline()
    pipeline_0.add_step(**step_0_data)
    pipeline_0.add_step(**step_1_data)
    pipeline_0.validate_step_types()

    step_0_data = {
        "name": "step_0",
        "processor": _return_same,
        "input_path": existing_input_file,
        "input_load_method": _load_sequence_dummy,
    }
    pipeline_1 = Pipeline()
    pipeline_1.add_step(**step_0_data)
    pipeline_1.add_step(**step_1_data)
    pipeline_1.validate_step_types()


@pytest.mark.xfail(
    reason="Validation does not currently support validating multiple outputs, but should be implemented in the future."
)
def test_pipeline_validate_types_multiple_outputs(existing_input_file: Path) -> None:

    step_0_data: dict[str, Any] = {
        "name": "step_0",
        "processor": _split_odd_even,
        "input_data": [1, 2, 3, 4, 5, 6],
        "outputs": ["odd", "even"],
    }
    step_1_data: dict[str, Any] = {
        "name": "step_1",
        "processor": _scale,
        "inputs": "step_0.odd",
        "params": {"factor": 10},
    }
    step_2_data: dict[str, Any] = {
        "name": "step_2",
        "processor": _scale,
        "inputs": "step_0.even",
        "params": {"factor": 20},
    }

    pipeline_0 = Pipeline()
    pipeline_0.add_step(**step_0_data)
    pipeline_0.add_step(**step_1_data)
    pipeline_0.add_step(**step_2_data)
    pipeline_0.validate_step_types()


@pytest.mark.parametrize(
    "step_data, expected_message",
    [
        (
            {
                "name": "step_invalid_input_file",
                "processor": _processor_str_sequence,
                "input_path": lf("existing_input_file"),
                "input_load_method": _load_sequence_dummy,
            },
            "Step 'step_invalid_input_file': Input types do not match processor inputs.",
        ),
        (
            {"name": "step_invalid_input", "processor": _processor_str_sequence, "inputs": "step_0"},
            "Step 'step_invalid_input': Input types do not match processor inputs.",
        ),
        (
            {"name": "step_missing_required_param", "processor": _scale, "inputs": "step_0", "params": {}},
            "Step 'step_missing_required_param': Required parameter 'factor' not provided in params.",
        ),
        (
            {
                "name": "step_invalid_param_name",
                "processor": _scale,
                "inputs": "step_0",
                "params": {"factor": 1, "factorio": "3"},
            },
            "Step 'step_invalid_param_name': Parameter 'factorio' not found in processor arguments.",
        ),
        (
            {"name": "step_invalid_param_type", "processor": _scale, "inputs": "step_0", "params": {"factor": "3"}},
            "Step 'step_invalid_param_type': Parameter 'factor' expected type <class 'int'>, got <class 'str'>.",
        ),
    ],
)
def test_pipeline_validate_types_fails(step_data: dict[str, Any], expected_message: str) -> None:
    step_0_data: dict[str, Any] = {
        "name": "step_0",
        "processor": _return_same,
        "input_data": [1, 2, 3],
    }
    pipeline = Pipeline()
    pipeline.add_step(**step_0_data)
    pipeline.add_step(**step_data)
    with pytest.raises(ValidationError, match=expected_message):
        pipeline.validate_step_types()
