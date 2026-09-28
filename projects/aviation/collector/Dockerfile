FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

COPY projects/aviation/collector/requirements.txt /tmp/collector-requirements.txt
RUN pip install --no-cache-dir -r /tmp/collector-requirements.txt

COPY projects/__init__.py /app/projects/__init__.py
COPY projects/aviation/__init__.py /app/projects/aviation/__init__.py
COPY projects/aviation/collector/__init__.py /app/projects/aviation/collector/__init__.py
COPY projects/aviation/collector/__main__.py /app/projects/aviation/collector/__main__.py
COPY projects/aviation/collector/opensky_to_minio.py /app/projects/aviation/collector/opensky_to_minio.py

USER 10001:10001
CMD ["python", "-m", "projects.aviation.collector", "--continuous"]
