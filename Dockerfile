# Wikipedia ETL pipeline - production / deployment image
# Python 3.13, uv for dependency management

FROM python:3.13-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Project files (README.md required by pyproject.toml for build)
COPY pyproject.toml uv.lock README.md ./
COPY src ./src

# Install dependencies and project (no dev extras)
RUN uv sync --no-dev

# Default: run the pipeline once
CMD ["uv", "run", "python", "-m", "etl_pipeline"]
