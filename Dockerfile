FROM python:3.11-slim

# Environment settings
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# System deps (minimal)
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m appuser

# Application source lives in /app
WORKDIR /app

# Install Python deps first (for build cache)
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy your code into the container
COPY . /app

# Make a writable directory for logs/output (mounted at runtime)
RUN mkdir -p /work && chown appuser:appuser /work

# Drop root privileges
USER appuser

# Run the loader as a module (requires loader/__init__.py)
ENTRYPOINT ["python", "-m", "loader.load"]
