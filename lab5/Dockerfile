FROM python:3.11-slim-bookworm
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONUTF8=1
WORKDIR /app
COPY lab5/requirements.lock.txt /app/lab5/requirements.lock.txt
RUN pip install --no-cache-dir -r /app/lab5/requirements.lock.txt
COPY lab3/src /app/lab3/src
COPY lab5/src /app/lab5/src
ENTRYPOINT ["python", "-B", "/app/lab5/src/worker.py"]
