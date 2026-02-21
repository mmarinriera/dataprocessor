import json
from pathlib import Path

import pytest

from dataprocessor.pipeline import Pipeline
from dataprocessor.pipeline import Step


class TestStep:
    def test_step_init(self):
        step = Step(
            name="test",
            processor=lambda x: x,
        )
        step.data = 5

    def test_step_access_data_before_solve(self):
        step = Step(
            name="test",
            processor=lambda x: x,
        )
        with pytest.raises(AttributeError, match="Step 'test': Attempted data retrieval before solving."):
            step.data


class TestPipeline:
    @pytest.mark.parametrize(
        "track_metadata, tracked_metadata_file",
        [
            (False, True),
            (True, True),
            (True, False),
        ],
    )
    def test_pipeline_init(self, track_metadata: bool, tracked_metadata_file: bool, tmp_path: Path):
        metadata_path = tmp_path / "metadata.json"
        target_tracked_metadata = (
            {"steps": {"step_1": {"name": "step_1"}}} if track_metadata and tracked_metadata_file else None
        )

        if tracked_metadata_file:
            with open(metadata_path, "w") as f:
                json.dump(target_tracked_metadata, f)

        pipeline = Pipeline(metadata_path=metadata_path if track_metadata else None)

        assert pipeline.tracked_metadata == target_tracked_metadata
