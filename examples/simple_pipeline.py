from dataprocessor.pipeline import Pipeline


def scale(input: list[int], factor: int) -> list[int]:
    """Scales the input list by a given factor."""
    return [x * factor for x in input]


def sort(input: list[int]) -> list[int]:
    """Sorts the input list."""
    return sorted(input)


def filter_by_threshold(input: list[int], threshold: int) -> list[int]:
    """Filters the input list by a given threshold."""
    return [x for x in input if x >= threshold]


def main():
    # Create a simple pipeline
    pipeline = Pipeline()

    pipeline.add_step(
        name="scaled",
        processor=scale,
        input_data=[5, 6, 4, 8, 6, 1],
        inputs=None,
        params={"factor": 2},
    )

    pipeline.add_step(
        name="sorted",
        processor=sort,
        inputs="scaled",
    )

    pipeline.add_step(
        name="filtered",
        processor=filter_by_threshold,
        inputs="sorted",
        params={"threshold": 10},
    )

    pipeline.run()

    # Run the pipeline with an initial input
    scaled = pipeline.get_output("scaled")
    sorted = pipeline.get_output("sorted")
    filtered = pipeline.get_output("filtered")

    print("Scaled:", scaled)
    print("Sorted:", sorted)
    print("Filtered:", filtered)

    assert scaled == [10, 12, 8, 16, 12, 2], "Scaled output is incorrect"
    assert sorted == [2, 8, 10, 12, 12, 16], "Sorted output is incorrect"
    assert filtered == [10, 12, 12, 16], "Filtered output is incorrect"


if __name__ == "__main__":
    main()
