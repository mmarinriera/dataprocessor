import csv
import os
from pathlib import Path

import pytest

from dataprocessor.step import Step

# Processors used in tests ###############################################


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


##########################################################################


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
