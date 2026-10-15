FROM python:3.12-slim-bookworm
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY streaming ./streaming
COPY ai ./ai
COPY models ./models
COPY schemas/iot-risk.avsc ./schemas/iot-risk.avsc
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "streaming.validator"]
