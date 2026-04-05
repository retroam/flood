FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY pyproject.toml README.md /app/
COPY src /app/src
COPY evals /app/evals

RUN pip install --no-cache-dir uv \
    && uv pip install --system .

EXPOSE 8000

CMD ["uvicorn", "quake_sql.main:app", "--host", "0.0.0.0", "--port", "8000"]
