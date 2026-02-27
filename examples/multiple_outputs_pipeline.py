import csv
import json
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


def save_int(value: int, filename: str | Path) -> None:
    with open(filename, "w") as f:
        f.write(f"{value}\n")


def load_int(filename: str | Path) -> int:
    with open(filename) as f:
        return int(f.read().strip())


def save_report_json(data: dict[str, int], filename: str | Path) -> None:
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def load_report_json(filename: str | Path) -> dict[str, int]:
    with open(filename) as f:
        return json.load(f)


####### Data processors #######


def split_cashflow(changes: list[int]) -> tuple[list[int], list[int]]:
    """
    Split daily cashflow changes into two outputs:
      - inflows: positive values
      - outflows: absolute value of negative values
    """
    inflows = [value for value in changes if value > 0]
    outflows = [abs(value) for value in changes if value < 0]
    return inflows, outflows


def total(values: list[int]) -> int:
    """Calculate the total sum of a list of integers."""
    return sum(values)


def max_value(values: list[int]) -> int:
    """Calculate the maximum value in a list of integers."""
    return max(values) if values else 0


def build_report(total_in: int, total_out: int, max_out: int) -> dict[str, int]:
    return {
        "total_inflows": total_in,
        "total_outflows": total_out,
        "net_balance": total_in - total_out,
        "largest_outflow": max_out,
    }


###############################


def main() -> None:

    data_path = Path("./examples/data/multiple_outputs/")

    logger = get_logger()
    logger.setLevel(logging.INFO)

    print("""*************************
Multiple-output pipeline:
    - First step returns a tuple with two outputs: inflows and outflows.
    - Downstream steps process each single output independently.
    - Each output is cached in its own file.
""")

    pipeline = Pipeline(force_run=False, metadata_path=data_path / "pipeline_metadata.json")

    pipeline.add_step(
        name="split_cashflow",
        processor=split_cashflow,
        input_path=data_path / "daily_changes.csv",
        input_load_method=load_sequence_csv,
        outputs=["inflows", "outflows"],
        output_path=(data_path / "inflows.csv", data_path / "outflows.csv"),
        load_method=load_sequence_csv,
        save_method=save_sequence_csv,
    )

    pipeline.add_step(
        name="total_inflows",
        processor=total,
        inputs="split_cashflow.inflows",
        output_path=data_path / "total_inflows.txt",
        load_method=load_int,
        save_method=save_int,
    )

    pipeline.add_step(
        name="total_outflows",
        processor=total,
        inputs="split_cashflow.outflows",
        output_path=data_path / "total_outflows.txt",
        load_method=load_int,
        save_method=save_int,
    )

    pipeline.add_step(
        name="largest_outflow",
        processor=max_value,
        inputs="split_cashflow.outflows",
        output_path=data_path / "largest_outflow.txt",
        load_method=load_int,
        save_method=save_int,
    )

    pipeline.add_step(
        name="cashflow_report",
        processor=build_report,
        inputs=["total_inflows", "total_outflows", "largest_outflow"],
        output_path=data_path / "cashflow_report.json",
        load_method=load_report_json,
        save_method=save_report_json,
    )

    try:
        pipeline.validate_step_types()
    except NotImplementedError:
        print("Type validation for multiple outputs is not yet supported. Skipping validation...")

    pipeline.run(parallel="process", max_workers=2, fail_fast=False)

    inflows, outflows = pipeline.get_output("split_cashflow")
    total_inflows = pipeline.get_output("total_inflows")
    total_outflows = pipeline.get_output("total_outflows")
    largest_outflow = pipeline.get_output("largest_outflow")
    report = pipeline.get_output("cashflow_report")

    print(f"""
Pipeline results:
    Inflows: {inflows}
    Outflows: {outflows}
    Total inflows: {total_inflows}
    Total outflows: {total_outflows}
    Largest outflow: {largest_outflow}
    Report: {report}
""")

    assert inflows == [200, 40, 300, 10, 90], "Inflows output is incorrect"
    assert outflows == [35, 120, 15, 70], "Outflows output is incorrect"
    assert total_inflows == 640, "Total inflows is incorrect"
    assert total_outflows == 240, "Total outflows is incorrect"
    assert largest_outflow == 120, "Largest outflow is incorrect"
    assert report["net_balance"] == 400, "Net balance is incorrect"


if __name__ == "__main__":
    main()
