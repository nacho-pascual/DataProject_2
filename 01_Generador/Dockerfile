# Use the official lightweight Python image.
FROM python:3.10-slim

ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
RUN mkdir app
WORKDIR app
COPY generator_docker.py generator.py
COPY requirements.txt requirements.txt

# Install dependencies.
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "generator.py"]