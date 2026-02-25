# DataProcessor

`dataprocessor` is a lightweight Python module for building data pipelines as dependency-aware steps.
It supports serial or parallel execution, optional file-based caching, and step type validation.

## Clone

```bash
git clone https://github.com/mmarinriera/dataprocessor.git
cd dataprocessor
```

## Installation

Using `pip`:

```bash
pip install -e .
```

For development dependencies:

```bash
pip install -e ".[dev]"
```

Using `uv`:

```bash
uv sync
```

With development dependencies:

```bash
uv sync --group dev
```

## Quick Example

```python
from dataprocessor import Pipeline


def multiply(values: list[int], factor: int) -> list[int]:
    return [value * factor for value in values]


def keep_even(values: list[int]) -> list[int]:
    return [value for value in values if value % 2 == 0]


pipeline = Pipeline(force_run=True)

pipeline.add_step(
    name="scaled",
    processor=multiply,
    input_data=[1, 2, 3, 4, 5],
    params={"factor": 3},
)

pipeline.add_step(
    name="even_only",
    processor=keep_even,
    inputs="scaled",
)

pipeline.validate_step_types()
pipeline.run()

print(pipeline.get_output("even_only"))  # [6, 12]
```
