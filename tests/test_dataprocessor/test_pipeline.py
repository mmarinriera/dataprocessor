import csv
import json
import threading
from pathlib import Path
from typing import Any

import pytest

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


def _save_sequence_csv(input: list[int], filename: str | Path) -> None:
    """Saves the input list as a CSV file."""
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(input)


def _scale(x: list[int], factor: int) -> list[int]:
    return [i * factor for i in x]


def _processor_str_sequence(x: list[str]) -> list[str]:
    return x


###########################


def test_step_init() -> None:
    step = Step(
        name="test",
        processor=lambda x: x,
    )
    step.data = 5


def test_step_access_data_before_solve() -> None:
    step = Step(
        name="test",
        processor=lambda x: x,
    )
    with pytest.raises(AttributeError, match="Step 'test': Attempted data retrieval before solving."):
        step.data


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
    step_data_0 = {
        "name": "step_0",
        "processor": some_processor,
        "params": {"some_param": 42},
        "input_data": [1, 2, 3],
    }
    pipeline.add_step(**step_data_0)  # type: ignore

    with subtests.test("Step in pipeline dict"):
        assert "step_0" in pipeline.steps

    with subtests.test("Step constructed correctly"):
        assert pipeline.steps["step_0"] == Step(**step_data_0)  # type: ignore

    step_data_1 = {
        "name": "step_1",
        "processor": lambda x: x,
        "inputs": "step_0",
        "output_path": "/some/output/path",
    }
    pipeline.add_step(**step_data_1)  # type: ignore

    with subtests.test("Step single input converted to list."):
        assert pipeline.steps["step_1"].inputs == ["step_0"]

    with subtests.test("Step Params dict is empty if not provided"):
        assert pipeline.steps["step_1"].params == {}

    step_data_2 = {
        "name": "step_2",
        "processor": lambda x: x,
        "inputs": ["step_0", "step_1"],
        "output_path": "/some/output/path",
    }
    pipeline.add_step(**step_data_2)  # type: ignore
    with subtests.test("Step multiple inputs remain as list."):
        assert pipeline.steps["step_2"].inputs == ["step_0", "step_1"]

    with subtests.test("Check pipeline metadata"):
        target_metadata = {
            "steps": {
                "step_0": {
                    "processor": "some_processor",
                    "inputs": [],
                    "params": {"some_param": 42},
                    "input_path": None,
                    "output_path": None,
                },
                "step_1": {
                    "processor": "<lambda>",
                    "params": {},
                    "inputs": ["step_0"],
                    "input_path": None,
                    "output_path": "/some/output/path",
                },
                "step_2": {
                    "processor": "<lambda>",
                    "params": {},
                    "inputs": ["step_0", "step_1"],
                    "input_path": None,
                    "output_path": "/some/output/path",
                },
            }
        }
        assert pipeline.metadata == target_metadata

    with subtests.test("Duplicated step name raises error"):
        with pytest.raises(ValueError, match="Step 'step_0' already exists in the pipeline."):
            pipeline.add_step(**step_data_0)  # type: ignore

    step_data_no_inputs = {"name": "step_no_inputs", "processor": lambda x: x, "params": {"some_param": 42}}
    with (
        subtests.test("Step without input_data"),
        pytest.raises(
            ValueError, match="Step 'step_no_inputs': must have either inputs, input data, or an input path."
        ),
    ):
        pipeline.add_step(**step_data_no_inputs)  # type: ignore

    step_no_load_method = {
        "name": "step_no_load_method",
        "processor": lambda x: x,
        "params": {"some_param": 42},
        "input_path": "/some/path",
    }
    with (
        subtests.test("Step with input_path but no load_method"),
        pytest.raises(
            ValueError, match="Step 'step_no_load_method': a load_method must be provided if input_path is specified."
        ),
    ):
        pipeline.add_step(**step_no_load_method)  # type: ignore


@pytest.mark.parametrize(
    "input_path, input_data, inputs, expected_output",
    [
        ("/some/path", [3, 2, 1], ["step_0"], [1, 2, 3, 4]),
        ("/some/path", None, ["step_0"], [1, 2, 3, 4]),
        ("/some/path", None, None, [1, 2, 3, 4]),
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
) -> None:

    step_0 = {
        "name": "step_0",
        "processor": _return_same,
        "input_data": [1, 2, 3],
    }

    step_1 = {
        "name": "step_1",
        "processor": _return_same,
        "input_path": input_path,
        "input_data": input_data,
        "inputs": inputs,
        "load_method": _load_sequence_dummy,
    }
    pipeline = Pipeline()
    pipeline.add_step(**step_0)  # type: ignore
    pipeline.add_step(**step_1)  # type: ignore
    pipeline.run()
    assert pipeline.get_output("step_1") == expected_output


def test_pipeline_run_autoload(tmp_path: Path, subtests: pytest.Subtests, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("DEBUG")
    input_path = tmp_path / "input.csv"
    with open(input_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([1, 2, 3])

    output_path_0 = tmp_path / "step_0_output.csv"
    output_path_1 = tmp_path / "step_1_output.csv"

    step_0 = {
        "name": "step_0",
        "processor": _return_same,
        "input_path": input_path,
        "output_path": output_path_0,
        "load_method": _load_sequence_csv,
        "save_method": _save_sequence_csv,
    }

    step_1 = {
        "name": "step_1",
        "processor": _return_same,
        "inputs": "step_0",
        "load_method": _load_sequence_csv,
        "save_method": _save_sequence_csv,
        "output_path": output_path_1,
    }

    pipeline_0 = Pipeline(force_run=True, metadata_path=tmp_path / "metadata.json")
    pipeline_0.add_step(**step_0)  # type: ignore
    pipeline_0.add_step(**step_1)  # type: ignore

    log_rerun = [
        f"Step 'step_0': Output file '{output_path_0}' not found or outdated. Recomputing step.",
        f"Step 'step_1': Output file '{output_path_1}' not found or outdated. Recomputing step.",
    ]

    with subtests.test("Pipeline runs for the first time, should execute all steps."):
        pipeline_0.run()
        assert all(msg not in caplog.text for msg in log_rerun)
    caplog.clear()

    pipeline_1 = Pipeline(force_run=True, metadata_path=tmp_path / "metadata.json")
    pipeline_1.add_step(**step_0)  # type: ignore
    pipeline_1.add_step(**step_1)  # type: ignore
    with subtests.test("Pipeline runs with force_run=True, should execute all steps."):
        pipeline_1.run()
        assert all(msg not in caplog.text for msg in log_rerun)
    caplog.clear()

    pipeline_2 = Pipeline(force_run=False, metadata_path=tmp_path / "metadata.json")
    pipeline_2.add_step(**step_0)  # type: ignore
    pipeline_2.add_step(**step_1)  # type: ignore

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
    pipeline_3.add_step(**step_0)  # type: ignore
    pipeline_3.add_step(**step_1)  # type: ignore
    with subtests.test("Pipeline runs with force_run=False but input file changed, should re-run steps"):
        pipeline_3.run()
        assert all(msg in caplog.text for msg in log_rerun)


def test_pipeline_autoload_metadata(
    tmp_path: Path, subtests: pytest.Subtests, caplog: pytest.LogCaptureFixture
) -> None:
    output_path_0 = tmp_path / "step_0_output.csv"

    step_0 = {
        "name": "step_0",
        "processor": _scale,
        "input_data": [1, 2, 3],
        "params": {"factor": 2},
        "output_path": output_path_0,
        "load_method": _load_sequence_csv,
        "save_method": _save_sequence_csv,
    }

    pipeline_0 = Pipeline(force_run=False, metadata_path=tmp_path / "metadata.json")
    pipeline_0.add_step(**step_0)  # type: ignore

    log_rerun = f"Step 'step_0': Output file '{output_path_0}' not found or outdated. Recomputing step."

    with subtests.test("Pipeline runs for the first time, should execute all steps."):
        pipeline_0.run()
        assert log_rerun in caplog.text
        assert pipeline_0.get_output("step_0") == [2, 4, 6]
    caplog.clear()

    step_0["params"] = {"factor": 3}
    pipeline_1 = Pipeline(force_run=False, metadata_path=tmp_path / "metadata.json")
    pipeline_1.add_step(**step_0)  # type: ignore
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

    pipeline.run(parallel=True, mode="thread", max_workers=2)

    assert pipeline.get_output("merge") == 5


def test_pipeline_run_parallel_invalid_mode() -> None:
    pipeline = Pipeline()
    pipeline.add_step(name="step", processor=lambda x: x, input_data=[1, 2, 3])

    with pytest.raises(ValueError, match="Invalid mode 'invalid'"):
        pipeline.run(parallel=True, mode="invalid")  # type: ignore[arg-type]


def test_pipeline_run_fail_fast_true_default() -> None:
    def source(x: int) -> int:
        return x

    def raises(_: int) -> int:
        raise ValueError("boom")

    pipeline = Pipeline()
    pipeline.add_step(name="source", processor=source, input_data=1)
    pipeline.add_step(name="fails", processor=raises, inputs="source")

    with pytest.raises(RuntimeError, match="Step 'fails' failed during pipeline execution."):
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
        pipeline.run(parallel=True, mode="thread", max_workers=2, fail_fast=False)

    assert pipeline.get_output("ok") == 2
    with pytest.raises(AttributeError, match="Step 'downstream': Attempted data retrieval before solving."):
        pipeline.get_output("downstream")


def test_pipeline_get_output(subtests: pytest.Subtests) -> None:
    step_0 = {
        "name": "step_0",
        "processor": _return_same,
        "input_data": [1, 2, 3],
    }
    pipeline = Pipeline()
    pipeline.add_step(**step_0)  # type: ignore
    with subtests.test("Attempting to get output before running pipeline"):
        with pytest.raises(AttributeError, match="Step 'step_0': Attempted data retrieval before solving."):
            pipeline.get_output("step_0")

    with subtests.test("Attempting to get output from a step not in the pipeline."):
        with pytest.raises(ValueError, match="Step 'step_1' does not exist in the pipeline."):
            pipeline.get_output("step_1")

    pipeline.run()
    with subtests.test("Getting output after running pipeline."):
        assert pipeline.get_output("step_0") == [1, 2, 3]


def test_pipeline_validate_types() -> None:
    step_0_data = {
        "name": "step_0",
        "processor": _return_same,
        "input_data": [1, 2, 3],
    }
    step_1_data = {
        "name": "step_1",
        "processor": _scale,
        "inputs": "step_0",
        "params": {"factor": 2},
    }
    pipeline_0 = Pipeline()
    pipeline_0.add_step(**step_0_data)  # type: ignore
    pipeline_0.add_step(**step_1_data)  # type: ignore
    pipeline_0.validate_step_types()

    step_0_data = {
        "name": "step_0",
        "processor": _return_same,
        "input_path": "/some/path.csv",
        "load_method": _load_sequence_dummy,
    }
    pipeline_1 = Pipeline()
    pipeline_1.add_step(**step_0_data)  # type: ignore
    pipeline_1.add_step(**step_1_data)  # type: ignore
    pipeline_1.validate_step_types()


@pytest.mark.parametrize(
    "step_data, expected_message",
    [
        (
            {
                "name": "step_invalid_input_file",
                "processor": _processor_str_sequence,
                "input_path": "some_file.csv",
                "load_method": _load_sequence_dummy,
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
    step_0_data = {
        "name": "step_0",
        "processor": _return_same,
        "input_data": [1, 2, 3],
    }
    pipeline = Pipeline()
    pipeline.add_step(**step_0_data)  # type: ignore
    pipeline.add_step(**step_data)  # type: ignore
    with pytest.raises(ValidationError, match=expected_message):
        pipeline.validate_step_types()
