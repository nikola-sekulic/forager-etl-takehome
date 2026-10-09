FROM python:3.12-slim-bookworm

WORKDIR /app

COPY pipeline/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline/ pipeline/
COPY bench/ bench/
COPY tests/ tests/

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/pipeline

CMD ["python", "-u", "/app/pipeline/main.py"]
