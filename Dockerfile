FROM apache/airflow:2.7.1-python3.11

COPY requirements.txt /opt/airflow/

USER root
RUN apt-get update && apt-get install -y gcc python3-dev && apt-get clean

USER airflow

# Install additional requirements (airflow is already in base image)
RUN pip install --no-cache-dir --prefer-binary \
    'boto3>=1.26.0' \
    'awswrangler>=3.0.0' \
    'redshift-connector>=2.1.0' \
    'openaq>=0.2.0' \
    'numpy<2.0.0' && \
    echo "[OK] All packages installed"