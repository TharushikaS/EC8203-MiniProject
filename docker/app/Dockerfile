# Lightweight Python image for the data-source simulators and the serving API.
FROM python:3.11-slim

ENV TZ=UTC PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/opt/smartgrid/src
WORKDIR /opt/smartgrid

COPY docker/app/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY src /opt/smartgrid/src
COPY scripts /opt/smartgrid/scripts

# Overridden per service in docker-compose.yml
CMD ["uvicorn", "smartgrid.serving.api:app", "--host", "0.0.0.0", "--port", "8080"]
