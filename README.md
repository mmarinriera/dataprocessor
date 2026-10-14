# DataProcessor

**A lightweight Python library for building dependency-aware data processing pipelines.**

DataProcessor lets you compose ordinary Python functions into structured workflows,
manage dependencies between processing steps, validate function signatures, and reuse persisted intermediate results.
Independent steps can run concurrently, making it useful for multi-stage data analysis and computational workflows.

## Features

- **Dependency-aware execution:** Define processing steps as a directed acyclic graph (DAG);
    DataProcessor determines their execution order.
- **Type validation:** Use Python type annotations to check whether step inputs and processor signatures
    are compatible before running a pipeline.
- **Persistent intermediate results:** Save step outputs to files and load them in subsequent
    runs to avoid unnecessary computation.
- **Parallel execution:** Run independent steps concurrently using threads or processes.
- **Flexible inputs and outputs:** Pass data directly, load it from files, connect steps by name,
    and expose multiple outputs from a single step.
- **Failure handling:** Track failed steps and skip dependent steps when their inputs are unavailable.

## Quick start

Install DataProcessor and define a pipeline using ordinary Python functions:

```bash
pip install git+https://github.com/mmarinriera/dataprocessor.git
```

```python
from dataprocessor import Pipeline


def scale(values: list[int], factor: int) -> list[int]:
    return [value * factor for value in values]


def filter_even(values: list[int]) -> list[int]:
    return [value for value in values if value % 2 == 0]


pipeline = Pipeline()

pipeline.add_step(
    name="scaled",
    processor=scale,
    input_data=[1, 2, 3, 4, 5],
    params={"factor": 3},
)

pipeline.add_step(
    name="filtered",
    processor=filter_even,
    inputs="scaled",
)

pipeline.validate_step_types()
pipeline.run()

print(pipeline.get_output("filtered"))
# [6, 12]
```

Each processing step defines a function, its inputs, and any additional parameters.
Subsequent steps can reference earlier outputs by name, allowing the workflow to be assembled incrementally.

## How it works

### 1. Define processing steps

Register a step with `Pipeline.add_step()`. A step can receive input data directly,
load data from a file, or consume the output of another step.

```python
pipeline.add_step(
    name="normalised",
    processor=normalise,
    inputs="filtered",
)
```

Additional keyword arguments are passed to the processor through `params`.

### 2. Validate the pipeline

Use type annotations on processor functions and call:

```python
pipeline.validate_step_types()
```

This checks the compatibility of declared input and output types across the pipeline,
helping identify configuration errors before computationally expensive work begins.

### 3. Execute the workflow

Run steps sequentially:

```python
pipeline.run()
```

Or enable parallel execution:

```python
pipeline.run(parallel="thread", max_workers=4)
```

You can also use `parallel="process"` for process-based execution.
DataProcessor respects dependencies between steps and schedules independent steps for concurrent execution.

### 4. Retrieve results

Access the result of any completed step by its name:

```python
result = pipeline.get_output("normalised")
```

Steps that return multiple outputs can expose individual results through named output references.

## Persistence and caching

DataProcessor supports saving intermediate results to files and loading them in subsequent runs.

Provide an `output_path`, a `save_method`, and a corresponding `load_method` when configuring a step. Set `force_run=False` when creating the pipeline to allow eligible persisted results to be reused.
Persisted results from a specific step will only be used if the container file is more recent than the data files from the corresponding dependency steps. In other words, if stored input data is more recent than the cached output for a certain step, the step will run and fresh output will be generated.

When pipeline metadata tracking is enabled through the `metadata_path` parameter, DataProcessor also records step configuration information and compares it with the previous run. If the metadata from a specific step changes from one run to the next, the cached result will be ignored and that step will be run.

**Important:** Cache validation is not a complete guarantee of reproducibility. In particular, changes to processor implementation, external dependencies, environment settings, or other hidden inputs may not be detected automatically. Users are responsible for ensuring that persisted outputs remain valid for their workflow.

## Installation

DataProcessor requires Python 3.10 or newer.

Install directly from GitHub using pip:

```bash
pip install git+https://github.com/mmarinriera/dataprocessor.git
```

Or add it to a project managed with uv:

```bash
uv add git+https://github.com/mmarinriera/dataprocessor.git
```

## Development

Clone the repository and install the development dependencies:

```bash
git clone https://github.com/mmarinriera/dataprocessor.git
cd dataprocessor
uv sync --group dev
```

Run the test suite and example workflows using tox:

```bash
uvx tox
```

The tox configuration covers tests across multiple Python versions, coverage reporting, example execution, and code-quality checks using prek.

Install the repository's pre-commit hooks with:

```bash
uv run prek install
uv run prek install --hook-type commit-msg
```

## Examples and API

See the [`examples/`](examples/) directory for additional runnable workflows, including text processing and multiple-output pipelines.

For implementation details, start with the [`src/dataprocessor/`](src/dataprocessor/) package and the tests in [`tests/`](tests/).

## License

DataProcessor is distributed under the MIT License. See [`LICENSE`](LICENSE) for details.
