FROM python:3.11-slim

WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir -e .

COPY . /app/mcp_server
ENV PYTHONPATH=/app

CMD ["python", "-m", "mcp_server.server"]
