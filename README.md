# DataProcess Flex

DataProcess Flex is a modular, extensible data processing toolkit for tabular and grouped data. The project supports interactive mode, command line subcommands, and JSON-based batch processing pipelines.

Repository: https://github.com/whb2468x/dataprocess_project

## Features

- Data overview and column profiling
- Data cleaning, normalization, and deduplication
- Conditional filtering, sorting, and column selection
- Derived column calculation using expressions
- Grouped aggregation and statistics
- Delimited value statistics for multi-value fields
- Cross-tabulation and pivot matrix building
- Table join, merge, and multi-dataset processing
- Long/wide table conversion, field splitting, and expansion
- Numeric normalization: relative, CPM, log2, log10, z-score, min-max
- Top-N selection and group comparison analysis
- JSON pipeline execution for repeatable batch workflows

## Installation

### Prerequisites

- Python >= 3.10
- numpy >= 1.26
- pandas >= 2.2

### Install from Source

```bash
git clone https://github.com/whb2468x/dataprocess_project.git
cd dataprocess_project
pip install -e .
```

## Usage

### Interactive Mode

Run without arguments to enter the interactive menu:

```bash
python dataprocess.py
```

After package installation, use:

```bash
dataprocess
```

### Command Line Mode

Use subcommands for specific operations. Example:

```bash
dataprocess inspect -i data.tsv
```

### Pipeline Mode

Create a JSON configuration and run:

```bash
dataprocess pipeline -c examples/pipeline_example.json
```

## Running Tests

```bash
python -m unittest discover -s tests
```

## Supported Data Formats

- CSV
- TSV
- TXT
- XLS / XLSX
- JSON
- Parquet
- PKL

Text files may auto-detect delimiters. Use `--sep` to set a delimiter explicitly.

## Project Structure

```
dataprocess.py                # Legacy entry point; interactive mode when no args
dataprocessor/                # Core package
├── cli.py                    # Command-line interface
├── interactive.py            # Interactive menu
├── pipeline.py               # JSON pipeline executor
├── session.py                # Data session management
├── io_utils.py               # Input/output utilities
├── utils.py                  # General utilities
└── operations/               # Operation modules
    ├── core.py
    ├── joins.py
    ├── stats.py
    └── __init__.py
examples/
└── pipeline_example.json     # Batch processing example
tests/
└── test_dataprocessor.py     # Unit tests
```

## Documentation

The repository also includes a Chinese-language user manual in `USER_MANUAL.md` with additional usage details.

## Contributing

Contributions are welcome. Please open issues, report bugs, or submit pull requests.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Release Notes

### v1.0.0
- Initial modular release
- Interactive, CLI, and pipeline execution modes
- Core data processing operations for cleaning, aggregation, joins, normalization, and export
- Multi-dataset session support
</content>
<parameter name="filePath">/home/whb/whb_projects/dataprocess_project/README.md