import json
from pathlib import Path

import pytest

from dataprocessor.pipeline import Pipeline
from dataprocessor.pipeline import Step


def test_step_init():
    step = Step(
        name="test",
        processor=lambda x: x,
    )
    step.data = 5


def test_step_access_data_before_solve():
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
def test_pipeline_init(track_metadata: bool, tracked_metadata_file: bool, tmp_path: Path):
    metadata_path = tmp_path / "metadata.json"
    target_tracked_metadata = (
        {"steps": {"step_1": {"name": "step_1"}}} if track_metadata and tracked_metadata_file else None
    )

    if tracked_metadata_file:
        with open(metadata_path, "w") as f:
            json.dump(target_tracked_metadata, f)

    pipeline = Pipeline(metadata_path=metadata_path if track_metadata else None)

    assert pipeline.tracked_metadata == target_tracked_metadata


def test_pipeline_add_step(subtests: pytest.Subtests):

    def some_processor(x):
        return x

    pipeline = Pipeline(metadata_path="dummy_path.json")
    step_data_0 = {
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

    step_data_1 = {
        "name": "step_1",
        "processor": lambda x: x,
        "inputs": ["step_0"],
        "output_path": "/some/output/path",
    }
    pipeline.add_step(**step_data_1)

    with subtests.test("Step Params dict is empty if not provided"):
        assert pipeline.steps["step_1"].params == {}

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
            }
        }
        assert pipeline.metadata == target_metadata

    with subtests.test("Duplicated step name raises error"):
        with pytest.raises(ValueError, match="Step 'step_0' already exists in the pipeline."):
            pipeline.add_step(**step_data_0)

    step_data_no_inputs = {"name": "step_2", "processor": lambda x: x, "params": {"some_param": 42}}
    with (
        subtests.test("Step without input_data"),
        pytest.raises(ValueError, match="Step 'step_2': must have either inputs, input data, or an input path."),
    ):
        pipeline.add_step(**step_data_no_inputs)

    step_no_load_method = {
        "name": "step_3",
        "processor": lambda x: x,
        "params": {"some_param": 42},
        "input_path": "/some/path",
    }
    with (
        subtests.test("Step with input_path but no load_method"),
        pytest.raises(ValueError, match="Step 'step_3': a load_method must be provided if input_path is specified."),
    ):
        pipeline.add_step(**step_no_load_method)
