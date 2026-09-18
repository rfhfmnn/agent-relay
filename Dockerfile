FROM python:3.11-slim

# Install uv from the official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set working directory
WORKDIR /app

# Set environment variables for Python and uv
ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1


# Copy dependencies first to leverage Docker layer caching
COPY pyproject.toml uv.lock ./

# Install project dependencies without dev packages
RUN uv sync --frozen --no-install-project --no-dev

# Ensure virtual environment binaries are on PATH
ENV PATH="/app/.venv/bin:$PATH"

# Copy application source code
COPY . .

# Expose API port
EXPOSE 8000

# Run FastAPI app with uvicorn listening on all interfaces
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
