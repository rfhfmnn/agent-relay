FROM python:3.11-slim

# Install uv from official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Configure Python and uv environment
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

# Copy dependency specifications first to leverage Docker layer caching
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Copy application source code
COPY . .

# Put the virtual environment in PATH
ENV PATH="/app/.venv/bin:$PATH"

# Expose the default API port
EXPOSE 8000

# Start Agent Relay API server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
