import csv
import logging
from pathlib import Path

from dataprocessor import Pipeline
from dataprocessor import get_logger

######### I/O methods #########


def save_sequence_csv(input: list[int], filename: str | Path) -> None:
    """Saves the input list as a CSV file."""
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(input)


def load_sequence_csv(filename: str | Path) -> list[int]:
    """Loads a list of integers from a CSV file."""
    with open(filename, newline="") as f:
        reader = csv.reader(f)
        return [int(x) for x in next(reader)]


####### Data processors #######


def scale(input: list[int], factor: int) -> list[int]:
    """Scales the input list by a given factor."""
    return [x * factor for x in input]


def sort(input: list[int]) -> list[int]:
    """Sorts the input list."""
    return sorted(input)


def filter_by_threshold(input: list[int], threshold: int) -> list[int]:
    """Filters the input list by a given threshold."""
    return [x for x in input if x >= threshold]


def filter_even(input: list[int]) -> list[int]:
    """Filters the input list to keep only even numbers."""
    return [x for x in input if x % 2 == 0]


def filter_odd(input: list[int]) -> list[int]:
    """Filters the input list to keep only odd numbers."""
    return [x for x in input if x % 2 != 0]


def merge(input1: list[int], input2: list[int]) -> list[int]:
    """Merges two input lists."""
    return input1 + input2


###############################


def main():

    logger = get_logger()
    logger.setLevel(logging.DEBUG)

    print("""Pipeline 1:
    - First step is initialised with data object.
    - The pipeline will check if the output files exist and load them instead of re-running the steps.

    """)
    pipeline_1 = Pipeline(force_run=False, metadata_path="./examples/data/pipeline_metadata.json")

    pipeline_1.add_step(
        name="scaled",
        processor=scale,
        input_data=[5, 4, 3, 2, 1, 0],
        inputs=None,
        params={"factor": 2},
        load_method=load_sequence_csv,
        save_method=save_sequence_csv,
        output_path="./examples/data/output_1_scaled.csv",
    )

    pipeline_1.add_step(
        name="sorted",
        processor=sort,
        inputs="scaled",
        load_method=load_sequence_csv,
        save_method=save_sequence_csv,
        output_path="./examples/data/output_1_sorted.csv",
    )

    pipeline_1.add_step(
        name="filtered",
        processor=filter_by_threshold,
        inputs="sorted",
        params={"threshold": 5},
        load_method=load_sequence_csv,
        save_method=save_sequence_csv,
        output_path="./examples/data/output_1_filtered.csv",
    )

    pipeline_1.validate_step_types()
    pipeline_1.run()

    scaled = pipeline_1.get_output("scaled")
    sorted = pipeline_1.get_output("sorted")
    filtered = pipeline_1.get_output("filtered")

    print(f"""
Pipeline 1 results:
    Scaled: {scaled}
    Sorted: {sorted}
    Filtered: {filtered}
""")

    assert scaled == [10, 8, 6, 4, 2, 0], "Scaled output is incorrect"
    assert sorted == [0, 2, 4, 6, 8, 10], "Sorted output is incorrect"
    assert filtered == [6, 8, 10], "Filtered output is incorrect"

    print("""*************************
Pipeline 2:
    - First step is initialised by loading data from a file.
    - No auto-loading of data files is enabled, so the steps will be re-run every time the pipeline is executed.          
    - Independent steps will be run in parallel.
""")

    pipeline_2 = Pipeline(force_run=True)

    pipeline_2.add_step(
        name="sorted",
        processor=sort,
        input_path="./examples/data/input_sequence.csv",
        load_method=load_sequence_csv,
        save_method=save_sequence_csv,
        output_path="./examples/data/output_2_sorted.csv",
    )

    pipeline_2.add_step(
        name="even_filtered",
        processor=filter_even,
        inputs="sorted",
        load_method=load_sequence_csv,
        save_method=save_sequence_csv,
        output_path="./examples/data/output_2_even_filtered.csv",
    )

    pipeline_2.add_step(
        name="odd_filtered",
        processor=filter_odd,
        inputs="sorted",
        load_method=load_sequence_csv,
        save_method=save_sequence_csv,
        output_path="./examples/data/output_2_odd_filtered.csv",
    )

    pipeline_2.add_step(
        name="even_scaled",
        processor=scale,
        inputs="even_filtered",
        params={"factor": 2},
        load_method=load_sequence_csv,
        save_method=save_sequence_csv,
        output_path="./examples/data/output_2_even_scaled.csv",
    )

    pipeline_2.add_step(
        name="odd_scaled",
        processor=scale,
        inputs="odd_filtered",
        params={"factor": 3},
        load_method=load_sequence_csv,
        save_method=save_sequence_csv,
        output_path="./examples/data/output_2_odd_scaled.csv",
    )

    pipeline_2.add_step(
        name="merged",
        processor=merge,
        inputs=["even_scaled", "odd_scaled"],
        load_method=load_sequence_csv,
        save_method=save_sequence_csv,
        output_path="./examples/data/output_2_merged.csv",
    )

    pipeline_2.validate_step_types()
    pipeline_2.run(parallel="process", max_workers=2, fail_fast=False)

    sorted = pipeline_2.get_output("sorted")
    even_filtered = pipeline_2.get_output("even_filtered")
    odd_filtered = pipeline_2.get_output("odd_filtered")
    even_scaled = pipeline_2.get_output("even_scaled")
    odd_scaled = pipeline_2.get_output("odd_scaled")
    merged = pipeline_2.get_output("merged")

    print(f"""
Pipeline 2 results:
    Sorted: {sorted}
    Even: {even_filtered}
    Odd: {odd_filtered}
    Even Scaled: {even_scaled}
    Odd Scaled: {odd_scaled}
    Merged: {merged}
""")

    assert even_filtered == [2, 4, 6, 8], "Even filtered  output is incorrect"
    assert odd_filtered == [1, 3, 5, 7, 9], "Odd filtered output is incorrect"
    assert even_scaled == [4, 8, 12, 16], "Even scaled output is incorrect"
    assert odd_scaled == [3, 9, 15, 21, 27], "Odd scaled output is incorrect"
    assert merged == [4, 8, 12, 16, 3, 9, 15, 21, 27], "Merged output is incorrect"


if __name__ == "__main__":
    main()
