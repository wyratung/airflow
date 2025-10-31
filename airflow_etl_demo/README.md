# BHYT ETL Pipeline

Complete ETL pipeline for BHYT (Bảo Hiểm Y Tế) data processing with Airflow.

## Project Structure

```
airflow_etl_demo/
├── dags/
│   ├── pipeline.py                      # Main Airflow DAG
│   ├── s3_loader.py                     # MinIO/S3 file loader
│   ├── bhyt_transformer_complete.py     # XML to DTO transformer
│   ├── rabbitmq_consumer.py             # RabbitMQ message consumer
│   └── XSD/                             # XSD schema files for validation
│       ├── GIAMDINHHS.xsd
│       ├── XML1_TONGHOP.xsd
│       ├── XML2_CHITIET_THUOC.xsd
│       ├── XML3_CHITIET_DVKT.xsd
│       ├── XML4_CHITIET_CLS.xsd
│       ├── XML5_DIENBIEN_LS.xsd
│       ├── XML6_CSDT_HIVAIDS.xsd
│       ├── XML7_GIAY_RA_VIEN.xsd
│       ├── XML8_TOM_TAT_HSBA.xsd
│       ├── XML9_GIAY_CHUNG_SINH.xsd
│       ├── XML10_GIAY_NGHI_THAI.xsd
│       ├── XML11_GIAY_NGHI_BHXH.xsd
│       ├── XML12_GIAM_DINH_YK.xsd
│       ├── XML13_CHUYEN_TUYEN.xsd
│       ├── XML14_HEN_KHAM.xsd
│       └── XML15_LAO.xsd
└── requirements.txt                     # Python dependencies

```

## Pipeline Flow

1. **LOAD** - Download XML from MinIO/S3 via RabbitMQ message
2. **VALIDATE** - Validate XML against XSD schemas
3. **TRANSFORM** - Transform XML to DTO structure
4. **VERIFY** - Verify data with external systems
5. **OUTPUT** - Generate JSON and send to API
6. **ACK** - Acknowledge RabbitMQ message

## Requirements

- Python 3.11.9
- Apache Airflow 2.7.1+
- MinIO (S3-compatible storage)
- RabbitMQ

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Set up Airflow variables (configure in Airflow UI or via CLI)
# MinIO variables:
# - minio_endpoint: 192.168.100.17:9000
# - minio_access_key: admin
# - minio_secret_key: 12345678
# - minio_bucket_name: demokcb-minhvd
# - minio_use_ssl: false

# RabbitMQ variables:
# - rabbitmq_host: localhost
# - rabbitmq_port: 5672
# - rabbitmq_username: guest
# - rabbitmq_password: guest
# - rabbitmq_exchange: xml.exchange
# - rabbitmq_queue: xml.queue
# - rabbitmq_routing_key: xml.key

# API variables:
# - bhyt_api_endpoint: https://localhost:44315/api/app/files/test
```

## Running the Pipeline

```bash
# Test run (standalone)
python dags/pipeline.py

# Run with Airflow
airflow dags trigger bhyt_complete_etl_pipeline_v2
```

## Configuration

All configurations are managed through Airflow Variables:

### MinIO Configuration
- Endpoint, credentials, and bucket settings

### RabbitMQ Configuration
- Host, port, credentials, exchange, queue, routing key

### API Configuration
- Target API endpoint for processed JSON output

## Dependencies

See `requirements.txt` for full list of Python packages.

Key dependencies:
- `apache-airflow` - Workflow orchestration
- `lxml` - XML processing and validation
- `pika` - RabbitMQ client
- `boto3` - AWS SDK (for S3/MinIO)
- `requests` - HTTP client for API calls
