FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
	PYTHONUNBUFFERED=1

WORKDIR /app

COPY aggregator/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY aggregator ./aggregator
COPY tests ./tests

RUN useradd --create-home --shell /bin/bash appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8080

CMD ["uvicorn", "aggregator.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
