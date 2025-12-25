# OpenAQ Data Pipeline Engineering

An end-to-end data engineering pipeline designed to extract, load and transform air quality data from the **OpenAQ API v3**. 
The system uses Apache Airflow for orchestration, AWS S3 for data lake storage, AWS Glue (PySpark) for transformation, Amazon Athena for querying, then visualize on Looker Studio (connected by OWOX)

## 🏗 Architecture

The end-to-end data pipeline architecture is as follows:

```
               Airflow (Docker Local)
         ─────────────────────────────────
         DAG:
           - openaq_to_athena_pipeline

                 │
                 ▼
        AWS Lambda: openaq-fetcher
        - Bước 1: /v3/locations (VN)
        - Bước 2: /v3/locations/{location_id}/measurements
        - Ghi raw JSON.gz vào S3 (aq_raw/)

                 │
                 ▼
      S3 Bucket: openaq-data-pipeline
      ├── aq_raw/    (raw zone: JSON.gz từ API)
      ├── aq_dev/    (dev zone: Parquet, test ETL)
      └── aq_prod/   (prod zone: Parquet, production)

                 │
                 ▼
               AWS Glue
      ├── Glue Jobs (dev/prod)
      │   - Đọc từ aq_raw/
      │   - Ghi Parquet vào aq_dev/ và aq_prod/
      └── Glue Crawlers (raw/dev/prod)
          - Cập nhật Glue Data Catalog

                 │
                 ▼
         Glue Data Catalog
      ├── raw_db   (tables mapping aq_raw/)
      ├── dev_db   (tables mapping aq_dev/)
      └── prod_db  (tables mapping aq_prod/)

                 │
                 ▼
            Amazon Athena
      ├── database: aq_dev
      │   └── table: vietnam
      └── database: aq_prod
          └── table: vietnam

                 │
                 ▼
                 OWOX
      ├── source: aq_dev.vietnam
      └── source: aq_prod.vietnam

                 │
                 ▼
            Looker Studio
      ├── dataset: aq_dev.vietnam
      └── dataset: aq_prod.vietnam
```

### S3 Data Lake Structure

The project utilizes a 3-zone architecture in S3:

1. **`aq_raw/`**: Immutable raw JSON data fetched directly from the OpenAQAPI (gzip compressed).
2. **`aq_dev/`**: Development zone for transformed Parquet files (partitioned by Year/Month/Day).
3. **`aq_prod/`**: Production zone for stable, reporting-ready data.

## 🔄 Data Flow

1. **Extraction**: Airflow triggers Python scripts to fetch location metadata and sensor measurements from OpenAQ API v3.
2. **Load**: Raw data is uploaded to S3 (`aq_raw`) in NDJSON format.
3. **Transformation**: An **AWS Glue PySpark job** is triggered. It:
   - Reads raw JSON files from `aq_raw`.
   - Cleanses and transforms the data (e.g., parsing dates, handling missing values).
   - Partitions the data by Year/Month/Day for efficient querying.
4. **Loading**: Transformed data is written to S3 (`aq_dev` or `aq_prod`) as partitioned Parquet files.
5. **Serving**: AWS Glue Crawler updates the Data Catalog, making the data queryable via Amazon Athena.

## 🛠 Tech Stack

* **Language:** Python 3.11
* **Orchestration:** Apache Airflow 2.7.1 (Dockerized)
* **Containerization:** Docker & Docker Compose
* **Cloud Provider:** AWS
* **Storage:** Amazon S3
* **Processing:** AWS Glue (PySpark 3.4.1), Pandas
* **Query Engine:** Amazon Athena
* **Infrastructure:** Manually provisioned (bucket, roles) via AWS Console.

## 📋 Prerequisites

1. **Docker Desktop** installed (with at least 4GB RAM allocated).
2. **AWS Account** with permissions for S3, Glue, and Athena.
3. **OpenAQ API Key** (Get one at [openaq.org](https://openaq.org/)).

## 🚀 Installation & Setup

### 1. Clone the Repository

```bash
git clone https://github.com/stevetran77/OpenAQ-Data-Pipeline-Engineering.git
cd OpenAQ-Data-Pipeline-Engineering

```

### 2. Configure AWS Resources

Follow the guide in `doc/AWS_CONFIG_GUIDE.md` to set up:

* S3 Bucket (e.g., `openaq-data-pipeline`)
* Glue Database (e.g., `openaq_dev`)
* IAM Roles for Glue and Athena

### 3. Set up Configuration

Create the configuration file from the example:

```bash
# Linux/Mac
cp config/config.conf.example config/config.conf

# Windows
copy config\config.conf.example config\config.conf

```

**Edit `config/config.conf**` and fill in your details:

* `[aws]`: Access Key, Secret Key, Region, Bucket Name.
* `[aws_glue]`: IAM Role ARN (from Step 2).
* `[api_keys]`: Your OpenAQ API Key.

### 4. Build and Start Docker

```bash
docker-compose build
docker-compose up -d

```

* **Airflow Webserver:** [http://localhost:8080](https://www.google.com/search?q=http://localhost:8080)
* **Username/Password:** `admin` / `admin`

## 🏃 Running the Pipeline

1. Access the Airflow UI at `http://localhost:8080`.
2. Locate the DAG named **`openaq_to_athena_pipeline`**.
3. Toggle the switch to **Unpause** the DAG.
4. Click the **Trigger DAG** (Play button) to start a manual run.

**The DAG performs the following steps:**

1. `extract_all_vietnam_locations`: Fetches data for all VN sensors.
2. `trigger_glue_transform_job`: Offloads processing to AWS Glue (Spark).
3. `wait_glue_transform_job`: Sensors the Glue job status.
4. `trigger_glue_crawler`: Catalogs the new Parquet files.
5. `validate_athena_data`: Checks if data is queryable.

## 🧪 Testing

You can run unit tests and integration tests inside the Docker container or locally.

```bash
# Run the full test suite
pytest tests/

# Test specific components
pytest tests/test_extract_data.py  # Test API extraction
pytest tests/test_glue_complete.py # Test PySpark logic locally
pytest tests/test_athena_connection.py # Validate AWS Connectivity

```

## 📂 Project Structure

```bash
.
├── config/                 # Configuration files (gitignored)
├── dags/                   # Airflow DAG definitions
│   ├── tasks/              # Task factories (extract, catalog, validate)
│   └── openaq_dag.py       # Main DAG file
├── doc/                    # Documentation (Architecture, Guides)
├── etls/                   # Logic for OpenAQ API extraction
├── glue_jobs/              # PySpark scripts uploaded to AWS Glue
│   └── process_openaq_raw.py
├── pipelines/              # High-level pipeline orchestration logic
├── tests/                  # Unit and integration tests
├── utils/                  # Shared utilities (AWS, Constants, Logging)
├── Dockerfile              # Custom Airflow image with PySpark/Java
├── docker-compose.yml      # Container orchestration
└── requirements.txt        # Python dependencies

```

## 📜 License

Distributed under the MIT License. See `LICENSE` for more information.

---

*Author: [Steve Tran*](https://www.google.com/search?q=https://github.com/stevetran77)