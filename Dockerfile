FROM python:3.10-slim

# Set work directory
WORKDIR /app

# Copy requirements and source code
COPY requirements.txt .

# Create virtual environment
RUN python -m venv .venv

# Upgrade pip and install dependencies in the venv
RUN .venv/bin/pip install --upgrade pip && \
    .venv/bin/pip install -r requirements.txt

# Activate venv by default
ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "--version"]