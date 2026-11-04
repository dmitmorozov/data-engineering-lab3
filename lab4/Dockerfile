FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY lab4/requirements.lock.txt /app/lab4/requirements.lock.txt
RUN pip install --no-cache-dir -r /app/lab4/requirements.lock.txt
COPY lab3/src /app/lab3/src
COPY lab3/configs /app/lab3/configs
COPY lab3/manifests/dataset.json /app/lab3/manifests/dataset.json
COPY lab4/src /app/lab4/src
ENTRYPOINT ["python", "-X", "utf8", "/app/lab4/src/coordinator.py"]
