# ducknx development commands

# list available recipes
default:
    @just --list

# install all dependencies including dev/test groups
setup:
    uv sync --all-extras --all-groups
    pre-commit install

# run ruff linter with auto-fix
lint:
    uv run ruff check --fix

# run ruff formatter
fmt:
    uv run ruff format

# run ruff linter and formatter
check: lint fmt

# run mypy type checking
typecheck:
    uv run mypy ducknx/

# run all pre-commit hooks
pre-commit:
    SKIP=no-commit-to-branch pre-commit run --all-files

# run unit tests (no PBF or network needed)
test-unit:
    uv run pytest tests/test_http.py tests/test_duckdb.py tests/test_pbf_reader.py tests/test_features_vectorized.py -v

# run integration tests (needs PBF file + internet)
test-integration:
    uv run pytest tests/test_osmnx.py -v --maxfail=1

# run all tests with coverage
test:
    uv run pytest --verbose --maxfail=1 --typeguard-packages=ducknx --cov=ducknx --cov-report=term-missing:skip-covered

# run tests in parallel (matches CI)
test-ci:
    uv run pytest --verbose --maxfail=1 --typeguard-packages=ducknx --cov=ducknx --cov-report=term-missing:skip-covered --numprocesses=3 --dist=loadgroup

# build the package
build:
    uv build

# validate the built package
validate: build
    twine check --strict ./dist/*
    validate-pyproject ./pyproject.toml

# serve docs locally
docs-serve:
    uv run mkdocs serve

# build docs
docs-build:
    uv run mkdocs build --strict

# full CI-style check: lint + build + test
ci: pre-commit validate test-ci

# clean temp files and build artifacts
clean:
    rm -rf .coverage* .pytest_cache .temp dist docs/build */__pycache__

# run pipeline benchmarks
bench:
    uv run python benchmarks/bench_pipeline.py
