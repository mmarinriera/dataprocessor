# DataProcessor

`dataprocessor` is a lightweight Python module for building structured and reproducible data processing pipelines
within in your Python project.

Pipelines are built by incrementally adding processing steps, which specify the input dependencies (as other steps),
the processor as such (as a pure Python function),
and the outputs of that step (which can be used as input to other steps).

Additional parameters can be passed to the processor if needed using the `params` argument,

```python
from dataprocessor import Pipeline


def multiply(values: list[int], factor: int) -> list[int]:
    return [value * factor for value in values]

pipeline = Pipeline(force_run=True)

pipeline.add_step(
    name="scaled",
    processor=multiply,
    input_data=[1, 2, 3, 4, 5],
    params={"factor": 3},
)
```

New steps may reference previous steps by name, for instance,

```python

def filter_even(values: list[int]) -> list[int]:
    return [value for value in values if value % 2 == 0]

pipeline.add_step(
    name="even_filtered",
    processor=filter_even,
    inputs="scaled",
)
```

Processing step outputs can be saved in a file, if a save method (i.e. a python function) is provided.
If the pipeline is re-run, the outputs may be loaded from file, thus skipping the step being executed,
if a loading method is provided (again, as a python function).

```python
def save_sequence_csv(input: list[int], filename: str | Path) -> None:
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(input)

def load_sequence_csv(filename: str | Path) -> list[int]:
    with open(filename, newline="") as f:
        reader = csv.reader(f)
        return [int(x) for x in next(reader)]

pipeline.add_step(
    name="even_scaled",
    processor=scale,
    inputs="even_filtered",
    params={factor: 2}
    save_method=save_sequence_csv,
    load_method=load_sequence_csv,
)
```

By using type annotations in the processor functions,
you can run a type check to make sure all step input and processor parameter
types match with the specified processor signatures.
This may be useful to catch bugs before running a time intensive data pipeline.

```python
pipeline.validate_step_types()
```

Pipeline execution supports parallelism to a certain degree.
Steps that are at the same level (relative to the DAG root) may be run in parallel,
but it will wait until all steps in the same level are finished, before starting to run the next steps.

```python
pipeline.run(parallel="process")
print(pipeline.get_output("even_scaled"))  # [12, 24]
```

## Install DataProcessor as a dependency in your project

Using `pip`:

```bash
pip install git+https://github.com/mmarinriera/dataprocessor
```
or, alternatively, add the following line to your `requirements.txt`:

```
git+https://github.com/mmarinriera/dataprocessor
```

Using `uv`:

```bash
uv add git+https://github.com/mmarinriera/dataprocessor
```

## Development

Clone the repository and install project,

```bash
git clone https://github.com/mmarinriera/dataprocessor.git
cd dataprocessor
uv sync --group dev
```
Use `pre-commit` or `prek` to install pre-commit hooks (formatting, linting, type-checking),

```bash
uv sync --group dev
prek install
prek install --hook-type commit-msg
```
and use `tox` to run tests with coverage.

## Full example

```python
import csv
from pathlib import Path

from dataprocessor import Pipeline

# I/O methods

def save_sequence_csv(input: list[int], filename: str | Path) -> None:
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(input)

def load_sequence_csv(filename: str | Path) -> list[int]:
    with open(filename, newline="") as f:
        reader = csv.reader(f)
        return [int(x) for x in next(reader)]

# Processor functions

def scale(values: list[int], factor: int) -> list[int]:
    return [value * factor for value in values]

def filter_even(values: list[int]) -> list[int]:
    return [value for value in values if value % 2 == 0]

######################

pipeline = Pipeline(force_run=True)

pipeline.add_step(
    name="scaled",
    processor=scale,
    input_data=[1, 2, 3, 4, 5],
    params={"factor": 3},
)

pipeline.add_step(
    name="even_filtered",
    processor=filter_even,
    inputs="scaled",
)

pipeline.add_step(
    name="even_scaled",
    processor=scale,
    inputs="even_filtered",
    params={"factor": 2}
    save_method=save_sequence_csv,
    load_method=load_sequence_csv,
)

pipeline.validate_step_types()
pipeline.run(parallel="process")
print(pipeline.get_output("even_scaled"))  # [12, 24]
```
Check more examples in the `examples` folder for an overview of all the features.
