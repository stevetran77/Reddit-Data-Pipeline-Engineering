# Dockerfile
FROM apache/airflow:2.7.1-python3.11

COPY requirements.txt /opt/airflow/

USER root
# [THÊM] Cài đặt Java (OpenJDK 17) và procps (cần cho Spark)
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-17-jdk-headless procps && \
    apt-get clean

# [THÊM] Thiết lập biến môi trường JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Install additional requirements
RUN pip install --no-cache-dir --prefer-binary \
    'boto3>=1.26.0' \
    'awswrangler>=3.0.0' \
    'redshift-connector>=2.1.0' \
    'openaq>=0.2.0' \
    'numpy<2.0.0' \
    'pyspark==3.4.1' && \
    echo "[OK] All packages installed"
# Lưu ý: PySpark 3.4.1 có hỗ trợ đầy đủ Python 3.11 (fix cloudpickle serialization bug)