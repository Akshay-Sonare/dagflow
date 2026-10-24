# my-project

A starter Python project with a package, a CLI, tests, and GitHub Actions CI.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Usage

```bash
my-project            # Hello, World!
my-project Akshay     # Hello, Akshay!
```

## Tests

```bash
pytest
```

## Layout

```
src/my_project/   package code (core.py, cli.py)
tests/            pytest tests
.github/workflows CI on every push / PR
```
