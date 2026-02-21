import csv
from pathlib import Path

from dataprocessor import Pipeline

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


###############################


def main():

    # Pipeline 1: First step is initialised with data object.
    pipeline_1 = Pipeline()

    pipeline_1.add_step(
        name="scaled",
        processor=scale,
        input_data=[5, 4, 3, 2, 1, 0],
        inputs=None,
        params={"factor": 2},
    )

    pipeline_1.add_step(
        name="sorted",
        processor=sort,
        inputs="scaled",
    )

    pipeline_1.add_step(
        name="filtered",
        processor=filter_by_threshold,
        inputs="sorted",
        params={"threshold": 5},
    )

    pipeline_1.validate_step_types()
    pipeline_1.run()

    scaled = pipeline_1.get_output("scaled")
    sorted = pipeline_1.get_output("sorted")
    filtered = pipeline_1.get_output("filtered")

    print("Pipeline 1 results:")
    print("Scaled:", scaled)
    print("Sorted:", sorted)
    print("Filtered:", filtered)

    assert scaled == [10, 8, 6, 4, 2, 0], "Scaled output is incorrect"
    assert sorted == [0, 2, 4, 6, 8, 10], "Sorted output is incorrect"
    assert filtered == [6, 8, 10], "Filtered output is incorrect"

    # Pipeline 2: First step is initialised by loading data from a file.
    pipeline_2 = Pipeline(force_run=False, metadata_path="./examples/data/pipeline_metadata.json")

    pipeline_2.add_step(
        name="scaled",
        processor=scale,
        params={"factor": 2},
        load_method=load_sequence_csv,
        input_path="./examples/data/input_sequence.csv",
        save_method=save_sequence_csv,
        output_path="./examples/data/scaled_output.csv",
    )

    pipeline_2.add_step(
        name="sorted",
        processor=sort,
        inputs="scaled",
        load_method=load_sequence_csv,
        save_method=save_sequence_csv,
        output_path="./examples/data/sorted_output.csv",
    )

    pipeline_2.add_step(
        name="filtered",
        processor=filter_by_threshold,
        inputs="sorted",
        params={"threshold": 10},
        load_method=load_sequence_csv,
        save_method=save_sequence_csv,
        output_path="./examples/data/filtered_output.csv",
    )

    pipeline_2.validate_step_types()
    pipeline_2.run()

    scaled = pipeline_2.get_output("scaled")
    sorted = pipeline_2.get_output("sorted")
    filtered = pipeline_2.get_output("filtered")

    print("Pipeline 2 results:")
    print("Scaled:", scaled)
    print("Sorted:", sorted)
    print("Filtered:", filtered)

    assert scaled == [10, 12, 8, 16, 12, 2], "Scaled output is incorrect"
    assert sorted == [2, 8, 10, 12, 12, 16], "Sorted output is incorrect"
    assert filtered == [10, 12, 12, 16], "Filtered output is incorrect"


if __name__ == "__main__":
    main()
