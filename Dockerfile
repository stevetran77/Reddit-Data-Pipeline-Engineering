FROM apache/airflow:2.7.1-python3.11

COPY requirements.txt /opt/airflow/

USER root
RUN apt-get update && apt-get install -y gcc python3-dev

USER airflow

# Install requirements with OpenAQ and AWS support
RUN pip install --no-cache-dir --prefer-binary -r /opt/airflow/requirements.txt

# Verify critical packages are installed
RUN python -c "import openaq; import boto3; import pyarrow" && echo "[OK] All critical packages installed"