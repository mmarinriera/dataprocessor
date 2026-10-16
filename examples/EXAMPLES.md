# Example scripts using DataProcessor

## How to run examples

Clone the repository and move to the root directory:

```bash
git clone https://github.com/mmarinriera/dataprocessor.git
cd dataprocessor
```

Create a virtual environment and install dataprocessor in it.

```bash
pip install git+https://github.com/mmarinriera/dataprocessor.git
```

Run the example:

```bash
python <path to example script>
```

The script [`image_processing.py`](image_processing.py) requires the additional dependencies: `numpy`, `matplotlib`, `scikit-image`.

The script [`sequence_analysis.py`](sequence_analysis.py) requires the additional dependencies: `biopython`, `pyyaml`.

### Using uv

Simply run:

```bash
uv run --with git+https://github.com/mmarinriera/dataprocessor.git [--with <additional dependencies>] <path to example script>
```

### Using tox

If you have set up the development environment, you can run all examples at once by running,

```bash
tox -e examples
```

## Data files

Input data and output files are saved in the [`data`](data) directory.

## Examples descriptions

- [`simple_pipeline.py`](simple_pipeline.py): Simple pipeline that manipulates sequences of integers. Shows how to use persisted data caching, and how to disable it.
- [`text_pipeline.py`:](text_pipeline.py): Simple pipeline applied to natural language processing. Uses persisted data caching.
- [`multiple_outputs_pipeline.py`](multiple_outputs_pipeline.py): Simple pipeline using steps with multiple outputs. Uses persisted data caching.
- [`image_processing.py`](image_processing.py): Simple image processing pipeline for grayscale fluorescence microscopy.  Requires additional dependencies.
- [`sequence_analysis.py`](sequence_analysis.py): Simple bioinformatics pipeline. Requires additional dependencies.
