FROM python:3.12-slim

RUN pip install --no-cache-dir uv==0.12.12

WORKDIR /app
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src ./src
RUN uv sync --frozen --no-dev

# Use the environment built above; do not add the dev group at start-up.
ENV UV_NO_SYNC=1

EXPOSE 8000
CMD ["uv", "run", "eventlake", "serve", "--host", "0.0.0.0", "--port", "8000"]
