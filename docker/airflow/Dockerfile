FROM apache/airflow:2.7.0

# Use non-interactive backend for matplotlib
ENV MPLBACKEND=Agg
USER root
# (Optional) system build tools if needed later
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
