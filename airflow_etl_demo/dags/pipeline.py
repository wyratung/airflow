"""
Complete BHYT ETL Pipeline với luồng:
Load -> Validate -> Transform -> Verify -> Output JSON
"""

from asyncio.log import logger
import tempfile
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
import logging
import json
import sys
import os
import re
import pathlib
from pathlib import Path
from lxml import etree
sys.path.insert(0, '/opt/airflow/plugins/helpers')

from s3_loader import S3FileLoader, create_minio_loader
from bhyt_transformer_complete import CompleteBHYTTransformer
from rabbitmq_consumer import RabbitMQConsumer, create_rabbitmq_consumer
# from bhyt_validator import CompleteBHYTValidator
# from external_verifier import ExternalSystemVerifier

# ============================================
# MinIO Configuration
# ============================================

MINIO_CONFIG = {
    'Endpoint': Variable.get('minio_endpoint', '192.168.100.17:9000'),
    'AccessKey': Variable.get('minio_access_key', 'admin'),
    'SecretKey': Variable.get('minio_secret_key', '12345678'),
    'BucketName': Variable.get('minio_bucket_name', 'demokcb-minhvd'),
    'XmlBucketName': Variable.get('minio_xml_bucket_name', 'demokcb-minhvd'),
    'XmlFolderPath': Variable.get('minio_xml_folder_path', 'XmlFiles'),
    'UseSSL': Variable.get('minio_use_ssl', 'false').lower() == 'true',
    'CreateBucketIfNotExists': True
}

# ============================================
# RabbitMQ Configuration
# ============================================

RABBITMQ_CONFIG = {
    'host': Variable.get('rabbitmq_host', 'localhost'),
    'port': int(Variable.get('rabbitmq_port', '5672')),
    'username': Variable.get('rabbitmq_username', 'guest'),
    'password': Variable.get('rabbitmq_password', 'guest'),
    'Exchange': Variable.get('rabbitmq_exchange', 'xml.exchange'),
    'Queue': Variable.get('rabbitmq_queue', 'xml.queue'),
    'RoutingKey': Variable.get('rabbitmq_routing_key', 'xml.key')
}

# ============================================
# API Configuration
# ============================================

API_CONFIG = {
    'endpoint': Variable.get('bhyt_api_endpoint', 'https://localhost:44315/api/app/files/test'),
    'timeout': int(Variable.get('bhyt_api_timeout', '60')),
    'max_retries': int(Variable.get('bhyt_api_max_retries', '3')),
    'verify_ssl': Variable.get('bhyt_api_verify_ssl', 'false').lower() == 'true'
}

# ============================================
# DAG Configuration
# ============================================

default_args = {
    'owner': 'bhyt_data_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 28),
    'email': ['datateam@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

dag = DAG(
    dag_id='bhyt_complete_etl_pipeline_v2',
    default_args=default_args,
    description='Complete ETL: Load -> Validate -> Transform -> Verify -> JSON Output',
    schedule_interval='*/15 * * * *',  # Chạy mỗi 15 phút
    catchup=False,
    max_active_runs=1,
    tags=['bhyt', 'qd4750', 'etl', 'production']
)


# ============================================
# TASK 1: LOAD - Download và Decode từ S3
# ============================================

def task_load_from_s3(**context):
    """
    TASK 1: Get message from RabbitMQ, download file từ S3, và validate XML theo GIAMDINHHS.xsd
    
    Input: Message từ RabbitMQ (XmlBigSizeMessageQueueDto)
    Output: Raw XML string (đã validate theo XSD)
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    execution_date = context['execution_date']
    
    logger.info("=" * 80)
    logger.info(f"TASK 1: LOAD FROM RABBITMQ & S3 - Starting at {execution_date}")
    logger.info("=" * 80)
    
    rabbitmq_consumer = None
    message = None
    
    try:
        # Step 1: Connect to RabbitMQ and get message
        logger.info("Step 1: Connecting to RabbitMQ...")
        
        rabbitmq_consumer = create_rabbitmq_consumer(RABBITMQ_CONFIG)
        rabbitmq_consumer.connect()
        
        # Check queue message count
        message_count = rabbitmq_consumer.get_queue_message_count()
        logger.info(f"Queue has {message_count} messages")
        
        if message_count == 0:
            logger.warning("No messages in queue - skipping this run")
            ti.xcom_push(key='skip_run', value=True)
            return {
                'status': 'skipped',
                'reason': 'no_messages_in_queue'
            }
        
        # Get one message from queue (manual ack)
        logger.info("Getting message from queue...")
        message = rabbitmq_consumer.get_message(auto_ack=False)
        
        if not message:
            logger.warning("Failed to get message from queue")
            ti.xcom_push(key='skip_run', value=True)
            return {
                'status': 'skipped',
                'reason': 'failed_to_get_message'
            }
        
        logger.info(f"OK Message received from RabbitMQ")
        logger.info(f"  Message ID: {message['id']}")
        logger.info(f"  S3 Path: {message['path']}")
        logger.info(f"  Delivery Tag: {message['delivery_tag']}")
        logger.info(f"  Redelivered: {message['redelivered']}")
        
        # Step 2: Parse S3 path
        logger.info("\nStep 2: Parsing S3 path...")
        logger.info(f"{message}")
        s3_key = message['path']
        bucket_name = MINIO_CONFIG['BucketName']
        # Parse s3://bucket/key format
        # if s3_path.startswith('s3://'):
        #     s3_path = s3_path[5:]  # Remove 's3://'
        
        # parts = s3_path.split('/', 1)
        # if len(parts) != 2:
        #     raise ValueError(f"Invalid S3 path format: {message['path']}")
        
        # bucket_name = parts[0]
        # s3_key = parts[1]
        
        logger.info(f"OK S3 Bucket: {bucket_name}")
        logger.info(f"OK S3 Key: {s3_key}")
        
        # Step 3: Download file from MinIO/S3
        logger.info("\nStep 3: Downloading file from MinIO/S3...")
        
        # Initialize MinIO loader
        logger.info(f"Connecting to MinIO: {MINIO_CONFIG['Endpoint']}")
        logger.info(f"Bucket: {bucket_name}")
        logger.info(f"Use SSL: {MINIO_CONFIG['UseSSL']}")
        
        loader = create_minio_loader(
            endpoint=MINIO_CONFIG['Endpoint'],
            access_key=MINIO_CONFIG['AccessKey'],
            secret_key=MINIO_CONFIG['SecretKey'],
            use_ssl=MINIO_CONFIG['UseSSL']
        )
        
        # Download and decode file
        xml_content = loader.download_and_decode_file(bucket_name, s3_key)
        
        logger.info(f"OK File downloaded and decoded successfully from MinIO")
        logger.info(f"OK XML content length: {len(xml_content)} characters")
        
        # Step 4: Validate XML theo GIAMDINHHS.xsd
        # Step 4: Validate XML theo GIAMDINHHS.xsd
        logger.info("\nStep 4: Validating XML against GIAMDINHHS.xsd schema...")
        
        # Load XSD schema
        xsd_path = Path(__file__).parent / "XSD" / "GIAMDINHHS.xsd"
        if not xsd_path.exists():
            logger.warning(f"XSD file not found at {xsd_path}")
            raise FileNotFoundError(f"GIAMDINHHS.xsd not found at {xsd_path}")
        
        with open(xsd_path, 'rb') as xsd_file:
            xsd_doc = etree.parse(xsd_file)
            xsd_schema = etree.XMLSchema(xsd_doc)
        
        logger.info(f"OK XSD schema loaded successfully from {xsd_path}")
        
        # Parse XML content
        # Remove BOM if exists
        if xml_content.startswith('\ufeff'):
            xml_content = xml_content[1:]
            logger.info("Removed BOM from XML content")
        
        xml_doc = etree.fromstring(xml_content.encode('utf-8'))
        
        # Validate XML against XSD
        is_valid = xsd_schema.validate(xml_doc)
        
        if is_valid:
            logger.info("OK XML is VALID according to GIAMDINHHS.xsd schema")
        else:
            logger.error("✗ XML is INVALID according to GIAMDINHHS.xsd schema")
            logger.error("Validation errors:")
            for error in xsd_schema.error_log:
                logger.error(f"  Line {error.line}: {error.message}")
            
            # Reject message and requeue
            if rabbitmq_consumer and message:
                rabbitmq_consumer.reject_message(
                    delivery_tag=message['delivery_tag'],
                    requeue=False  # Don't requeue invalid XML
                )
            
            # Raise exception nếu không valid
            error_messages = [f"Line {err.line}: {err.message}" for err in xsd_schema.error_log]
            raise AirflowFailException(
                f"XML validation failed against GIAMDINHHS.xsd:\n" + "\n".join(error_messages[:5])
            )
        
        # Step 5: Basic structure check
        # Step 5: Basic structure check
        logger.info("\nStep 5: Performing basic structure check...")
        
        # Check root element
        if xml_doc.tag != 'GIAMDINHHS':
            raise ValueError(f"Invalid root element: {xml_doc.tag}, expected GIAMDINHHS")
        
        # Check THONGTINDONVI
        thongtindonvi = xml_doc.find('THONGTINDONVI')
        if thongtindonvi is not None:
            ma_lk = thongtindonvi.findtext('MA_LK')
            logger.info(f"OK Found MA_LK: {ma_lk}")
        else:
            logger.warning("THONGTINDONVI element not found")
            ma_lk = None
        
        # Check THONGTINHOSO
        thongtinhoso = xml_doc.find('THONGTINHOSO')
        if thongtinhoso is None:
            raise ValueError("Missing THONGTINHOSO element")
        
        ngaylap = thongtinhoso.findtext('NGAYLAP')
        soluonghoso = thongtinhoso.findtext('SOLUONGHOSO')
        logger.info(f"OK NGAYLAP: {ngaylap}")
        logger.info(f"OK SOLUONGHOSO: {soluonghoso}")
        
        # Check DANHSACHHOSO
        danhsachhoso = thongtinhoso.find('DANHSACHHOSO')
        if danhsachhoso is None:
            raise ValueError("Missing DANHSACHHOSO element")
        
        hoso_list = danhsachhoso.findall('HOSO')
        logger.info(f"OK Found {len(hoso_list)} HOSO elements")
        
        # Count FILEHOSO in each HOSO
        total_filehoso = 0
        for idx, hoso in enumerate(hoso_list, 1):
            filehoso_list = hoso.findall('FILEHOSO')
            total_filehoso += len(filehoso_list)
            logger.info(f"  HOSO #{idx}: {len(filehoso_list)} FILEHOSO elements")
            
            # Log LOAIHOSO types (không decode NOIDUNGFILE)
            loaihoso_types = [fh.findtext('LOAIHOSO') for fh in filehoso_list]
            logger.info(f"    LOAIHOSO: {', '.join(loaihoso_types)}")
        
        logger.info(f"OK Total FILEHOSO elements: {total_filehoso}")
        
        # Step 6: Push data to XCom
        # Step 6: Push data to XCom
        logger.info("\nStep 6: Pushing data to XCom...")
        
        # Push raw XML content
        ti.xcom_push(key='raw_xml_content', value=xml_content)
        
        # Push metadata
        metadata = {
            'message_id': message['id'],
            'rabbitmq_delivery_tag': message['delivery_tag'],
            's3_bucket': bucket_name,
            's3_key': s3_key,
            's3_uri': f"s3://{bucket_name}/{s3_key}",
            'file_size': len(xml_content),
            'received_at': message['received_at'],
            'redelivered': message['redelivered'],
            'message_raw': message['message_raw'],
            'xsd_validation': {
                'schema': 'GIAMDINHHS.xsd',
                'is_valid': True,
                'validated_at': datetime.utcnow().isoformat()
            },
            'structure_info': {
                'ma_lk': ma_lk,
                'ngaylap': ngaylap,
                'soluonghoso': soluonghoso,
                'total_hoso': len(hoso_list),
                'total_filehoso': total_filehoso
            }
        }
        
        ti.xcom_push(key='file_metadata', value=metadata)
        
        # Step 7: Delete message from RabbitMQ queue immediately after successful load
        logger.info("\nStep 7: Deleting message from RabbitMQ queue...")
        
        rabbitmq_message_deleted = False
        
        try:
            logger.info(f"Load successful - attempting to delete RabbitMQ message (delivery_tag: {message['delivery_tag']})")
            
            # Acknowledge message (this removes it from queue)
            rabbitmq_consumer.acknowledge_message(message['delivery_tag'])
            
            rabbitmq_message_deleted = True
            
            logger.info("OK RabbitMQ message deleted successfully after successful load")
            
        except Exception as rabbitmq_error:
            logger.error(f"✗ Failed to delete RabbitMQ message: {str(rabbitmq_error)}")
            logger.warning("Message will remain in queue and may be redelivered")
        
        logger.info("=" * 80)
        logger.info("TASK 1: LOAD FROM RABBITMQ & S3 - Completed successfully")
        logger.info(f"OK Message ID: {message['id']}")
        logger.info(f"OK S3 URI: s3://{bucket_name}/{s3_key}")
        logger.info(f"OK XSD Validation: PASSED")
        logger.info(f"OK MA_LK: {ma_lk}")
        logger.info(f"OK HOSO count: {len(hoso_list)}")
        logger.info(f"OK FILEHOSO count: {total_filehoso}")
        logger.info(f"OK RabbitMQ Message Deleted: {rabbitmq_message_deleted}")
        logger.info("=" * 80)
        
        return {
            'status': 'success',
            'message_id': message['id'],
            'xsd_validation': 'passed',
            'file_size': len(xml_content),
            's3_uri': f"s3://{bucket_name}/{s3_key}",
            'hoso_count': len(hoso_list),
            'filehoso_count': total_filehoso,
            'rabbitmq_message_deleted': rabbitmq_message_deleted
        }
        
    except Exception as e:
        logger.error(f"TASK 1: LOAD FROM RABBITMQ & S3 - Failed with error: {str(e)}")
        logger.exception(e)
        
        # Reject message on error (requeue based on error type)
        if rabbitmq_consumer and message:
            try:
                # Don't requeue if it's a validation error or parsing error
                requeue = not isinstance(e, (ValueError, AirflowFailException))
                rabbitmq_consumer.reject_message(
                    delivery_tag=message['delivery_tag'],
                    requeue=requeue
                )
                logger.info(f"Message rejected (requeue={requeue})")
            except Exception as ack_error:
                logger.error(f"Failed to reject message: {str(ack_error)}")
        
        raise AirflowFailException(f"Load task failed: {str(e)}")
    
    finally:
        # Close RabbitMQ connection
        if rabbitmq_consumer:
            try:
                rabbitmq_consumer.close()
            except Exception as close_error:
                logger.warning(f"Failed to close RabbitMQ connection: {str(close_error)}")


# ============================================
# TASK 2: VALIDATE - Validate Nested XML với XSD
# ============================================

def task_validate_xml(**context):
    """
    TASK 2: Validate nested XML với XSD tương ứng
    
    Decode từng nested XML trong <NOIDUNGFILE> và validate với file XSD
    tương ứng dựa trên giá trị <LOAIHOSO> (XML1, XML2, XML3, v.v.)
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("=" * 80)
    logger.info("TASK 2: VALIDATE NESTED XML - Starting")
    logger.info("=" * 80)
    
    try:
        # Step 1: Pull XML content
        logger.info("Step 1: Pulling XML content from previous task...")
        
        xml_content = ti.xcom_pull(task_ids='load_from_s3', key='raw_xml_content')
        file_metadata = ti.xcom_pull(task_ids='load_from_s3', key='file_metadata')
        
        if not xml_content:
            raise ValueError("No XML content received from previous task")
        
        logger.info(f"XML content size: {len(xml_content)} characters")
        
        # Step 2: Parse wrapper XML
        logger.info("Step 2: Parsing wrapper XML...")
        
        # Remove BOM if exists
        if xml_content.startswith('\ufeff'):
            xml_content = xml_content[1:]
            logger.info("Removed BOM from XML content")
        
        # Parse với lxml
        root = etree.fromstring(xml_content.encode('utf-8'))
        logger.info(f"OK Wrapper XML parsed successfully. Root element: {root.tag}")
        
        # Step 3: Prepare XSD schema directory
        logger.info("Step 3: Preparing XSD schema directory...")
        
        xsd_dir = Path(__file__).parent / "XSD"
        if not xsd_dir.exists():
            raise FileNotFoundError(f"XSD directory not found: {xsd_dir}")
        
        logger.info(f"OK XSD directory: {xsd_dir}")
        
        # XSD mapping: LOAIHOSO -> XSD filename
        # XSD mapping: LOAIHOSO -> XSD filename
        # Updated to match actual XSD files and transformer structure
        xsd_mapping = {
            'XML1': 'XML1_TONGHOP.xsd',                    # TONG_HOP (Object)
            'XML2': 'XML2_CHITIET_THUOC.xsd',              # DSACH_CHI_TIET_THUOC (Array)
            'XML3': 'XML3_CHITIET_DVKT.xsd',               # DSACH_CHI_TIET_DVKT (Array)
            'XML4': 'XML4_CHITIET_CLS.xsd',                # DSACH_CHI_TIET_CLS (Array)
            'XML5': 'XML5_DIENBIEN_LS.xsd',                # DSACH_CHI_TIET_DIEN_BIEN_BENH (Array)
            'XML6': 'XML6_CSDT_HIVAIDS.xsd',               # DSACH_HO_SO_HIV_AIDS (Array)
            'XML7': 'XML7_GIAY_RA_VIEN.xsd',               # GIAY_RA_VIEN (Object)
            'XML8': 'XML8_TOM_TAT_HSBA.xsd',               # TOM_TAT_HO_SO_BA (Object)
            'XML9': 'XML9_GIAY_CHUNG_SINH.xsd',            # GIAY_CHUNG_SINH (Object)
            'XML10': 'XML10_GIAY_NGHI_THAI.xsd',           # GIAY_NGHI_DUONG_THAI (Object)
            'XML11': 'XML11_GIAY_NGHI_BHXH.xsd',           # GIAY_NGHI_VIEC_HUONG_BHXH (Object)
            'XML12': 'XML12_GIAM_DINH_YK.xsd',             # DSACH_GIAM_DINH_Y_KHOA (Array)
            'XML13': 'XML13_CHUYEN_TUYEN.xsd',             # DSACH_GIAY_CHUYEN_TUYEN (Array)
            'XML14': 'XML14_HEN_KHAM.xsd',                 # DSACH_GIAY_HEN_KHAM_LAI (Array)
            'XML15': 'XML15_LAO.xsd'                       # DSACH_THONG_TIN_LAO (Array)
        }
        
        # Step 4: Process each HOSO and validate nested XML
        logger.info("Step 4: Processing HOSO and validating nested XML...")
        
        validation_errors = []
        validation_warnings = []
        validation_details = []
        
        # Find all HOSO elements
        thongtinhoso = root.find('THONGTINHOSO')
        if thongtinhoso is None:
            raise ValueError("Missing THONGTINHOSO element")
        
        danhsachhoso = thongtinhoso.find('DANHSACHHOSO')
        if danhsachhoso is None:
            raise ValueError("Missing DANHSACHHOSO element")
        
        hoso_list = danhsachhoso.findall('HOSO')
        logger.info(f"Found {len(hoso_list)} HOSO elements")
        
        total_validated = 0
        total_passed = 0
        total_failed = 0
        
        # Process each HOSO
        for hoso_idx, hoso in enumerate(hoso_list, 1):
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing HOSO #{hoso_idx}")
            logger.info(f"{'='*60}")
            
            filehoso_list = hoso.findall('FILEHOSO')
            logger.info(f"Found {len(filehoso_list)} FILEHOSO in HOSO #{hoso_idx}")
            
            # Extract MA_LK from XML1 (TONG_HOP) for this HOSO
            ma_lk_for_hoso = None
            try:
                # Find XML1 (TONG_HOP) to get MA_LK
                for fh in filehoso_list:
                    if fh.findtext('LOAIHOSO') == 'XML1':
                        noidung_xml1 = fh.findtext('NOIDUNGFILE', '').strip()
                        if noidung_xml1:
                            import base64
                            decoded_xml1 = base64.b64decode(noidung_xml1).decode('utf-8')
                            if decoded_xml1.startswith('\ufeff'):
                                decoded_xml1 = decoded_xml1[1:]
                            xml1_root = etree.fromstring(decoded_xml1.encode('utf-8'))
                            ma_lk_for_hoso = xml1_root.findtext('.//MA_LK') or xml1_root.findtext('MA_LK')
                            break
            except Exception as e:
                logger.warning(f"Could not extract MA_LK from HOSO #{hoso_idx}: {str(e)}")
            
            logger.info(f"MA_LK for HOSO #{hoso_idx}: {ma_lk_for_hoso or 'N/A'}")
            
            # Process each FILEHOSO
            for filehoso_idx, filehoso in enumerate(filehoso_list, 1):
                loaihoso = filehoso.findtext('LOAIHOSO')
                noidungfile_base64 = filehoso.findtext('NOIDUNGFILE', '').strip()
                
                if not loaihoso:
                    validation_warnings.append(f"HOSO #{hoso_idx} (MA_LK: {ma_lk_for_hoso or 'N/A'}), FILEHOSO #{filehoso_idx}: Missing LOAIHOSO")
                    continue
                
                logger.info(f"\n  Processing FILEHOSO #{filehoso_idx}: {loaihoso}")
                
                if not noidungfile_base64:
                    validation_warnings.append(f"HOSO #{hoso_idx} (MA_LK: {ma_lk_for_hoso or 'N/A'}), {loaihoso}: Empty NOIDUNGFILE")
                    logger.warning(f"  ⚠ {loaihoso}: Empty NOIDUNGFILE")
                    continue
                
                try:
                    # Decode base64 content
                    logger.info(f"  Decoding base64 content ({len(noidungfile_base64)} chars)...")
                    
                    import base64
                    decoded_bytes = base64.b64decode(noidungfile_base64)
                    nested_xml = decoded_bytes.decode('utf-8')
                    
                    # Remove BOM from nested XML if exists
                    if nested_xml.startswith('\ufeff'):
                        nested_xml = nested_xml[1:]
                    
                    logger.info(f"  OK Decoded to {len(nested_xml)} characters")
                    
                    # Get corresponding XSD file
                    xsd_filename = xsd_mapping.get(loaihoso)
                    if not xsd_filename:
                        validation_warnings.append(f"HOSO #{hoso_idx} (MA_LK: {ma_lk_for_hoso or 'N/A'}), {loaihoso}: No XSD mapping found")
                        logger.warning(f"  ⚠ No XSD mapping for {loaihoso}")
                        continue
                    
                    xsd_path = xsd_dir / xsd_filename
                    if not xsd_path.exists():
                        validation_warnings.append(f"HOSO #{hoso_idx} (MA_LK: {ma_lk_for_hoso or 'N/A'}), {loaihoso}: XSD file not found ({xsd_filename})")
                        logger.warning(f"  ⚠ XSD file not found: {xsd_path}")
                        continue
                    
                    # Load XSD schema
                    logger.info(f"  Loading XSD schema: {xsd_filename}")
                    with open(xsd_path, 'rb') as xsd_file:
                        xsd_doc = etree.parse(xsd_file)
                        xsd_schema = etree.XMLSchema(xsd_doc)
                    
                    # Parse nested XML
                    logger.info(f"  Parsing nested XML...")
                    nested_root = etree.fromstring(nested_xml.encode('utf-8'))
                    logger.info(f"  OK Parsed nested XML: <{nested_root.tag}>")
                    
                    # Validate nested XML against XSD
                    logger.info(f"  Validating against {xsd_filename}...")
                    is_valid = xsd_schema.validate(nested_root)
                    
                    total_validated += 1
                    
                    if is_valid:
                        total_passed += 1
                        logger.info(f"  OK {loaihoso} validation PASSED")
                        
                        validation_details.append({
                            'hoso_index': hoso_idx,
                            'ma_lk': ma_lk_for_hoso,
                            'filehoso_index': filehoso_idx,
                            'loaihoso': loaihoso,
                            'xsd_file': xsd_filename,
                            'status': 'PASSED',
                            'root_element': nested_root.tag,
                            'xml_size': len(nested_xml)
                        })
                    else:
                        total_failed += 1
                        logger.error(f"  ✗ {loaihoso} validation FAILED")
                        
                        # Collect validation errors
                        error_messages = []
                        for error in xsd_schema.error_log:
                            error_msg = f"Line {error.line}: {error.message}"
                            error_messages.append(error_msg)
                            logger.error(f"    {error_msg}")
                        
                        validation_errors.append({
                            'hoso_index': hoso_idx,
                            'ma_lk': ma_lk_for_hoso,
                            'filehoso_index': filehoso_idx,
                            'loaihoso': loaihoso,
                            'xsd_file': xsd_filename,
                            'error_messages': error_messages[:5]  # First 5 errors
                        })
                        
                        validation_details.append({
                            'hoso_index': hoso_idx,
                            'ma_lk': ma_lk_for_hoso,
                            'filehoso_index': filehoso_idx,
                            'loaihoso': loaihoso,
                            'xsd_file': xsd_filename,
                            'status': 'FAILED',
                            'root_element': nested_root.tag,
                            'xml_size': len(nested_xml),
                            'error_count': len(xsd_schema.error_log)
                        })
                    
                except base64.binascii.Error as decode_error:
                    validation_errors.append({
                        'hoso_index': hoso_idx,
                        'ma_lk': ma_lk_for_hoso,
                        'filehoso_index': filehoso_idx,
                        'loaihoso': loaihoso,
                        'error_messages': [f"Base64 decode error: {str(decode_error)}"]
                    })
                    logger.error(f"  ✗ Base64 decode error: {str(decode_error)}")
                    
                except etree.XMLSyntaxError as parse_error:
                    validation_errors.append({
                        'hoso_index': hoso_idx,
                        'ma_lk': ma_lk_for_hoso,
                        'filehoso_index': filehoso_idx,
                        'loaihoso': loaihoso,
                        'error_messages': [f"XML parse error: {str(parse_error)}"]
                    })
                    logger.error(f"  ✗ XML parse error: {str(parse_error)}")
                    
                except Exception as nested_error:
                    validation_errors.append({
                        'hoso_index': hoso_idx,
                        'ma_lk': ma_lk_for_hoso,
                        'filehoso_index': filehoso_idx,
                        'loaihoso': loaihoso,
                        'error_messages': [f"Unexpected error: {str(nested_error)}"]
                    })
                    logger.error(f"  ✗ Unexpected error: {str(nested_error)}")
        
        # Step 5: Decode all nested XML in wrapper XML
        logger.info("\n" + "=" * 80)
        logger.info("Step 5: Decoding all nested XML in xml_content...")
        logger.info("=" * 80)
        
        # Parse xml_content để modify trực tiếp
        root_to_modify = etree.fromstring(xml_content.encode('utf-8'))
        
        # Find all FILEHOSO and replace base64 with decoded XML
        thongtinhoso_modify = root_to_modify.find('THONGTINHOSO')
        if thongtinhoso_modify is not None:
            danhsachhoso_modify = thongtinhoso_modify.find('DANHSACHHOSO')
            if danhsachhoso_modify is not None:
                hoso_list_modify = danhsachhoso_modify.findall('HOSO')
                
                decode_count = 0
                for hoso in hoso_list_modify:
                    filehoso_list_modify = hoso.findall('FILEHOSO')
                    
                    for filehoso in filehoso_list_modify:
                        loaihoso = filehoso.findtext('LOAIHOSO')
                        noidungfile_elem = filehoso.find('NOIDUNGFILE')
                        
                        if noidungfile_elem is not None and noidungfile_elem.text:
                            noidungfile_base64 = noidungfile_elem.text.strip()
                            
                            if noidungfile_base64:
                                try:
                                    # Decode base64
                                    import base64
                                    decoded_bytes = base64.b64decode(noidungfile_base64)
                                    nested_xml = decoded_bytes.decode('utf-8')
                                    
                                    # Remove BOM if exists
                                    if nested_xml.startswith('\ufeff'):
                                        nested_xml = nested_xml[1:]
                                    
                                    # Remove XML declaration from nested XML (để tránh conflict)
                                    nested_xml = re.sub(r'<\?xml[^?]*\?>\s*', '', nested_xml)
                                    
                                    # Clear existing content
                                    noidungfile_elem.text = None
                                    noidungfile_elem.tail = None
                                    # Remove all children
                                    for child in list(noidungfile_elem):
                                        noidungfile_elem.remove(child)
                                    
                                    # Parse nested XML and add as child elements
                                    try:
                                        nested_root = etree.fromstring(nested_xml.encode('utf-8'))
                                        # Append nested XML as child of NOIDUNGFILE
                                        noidungfile_elem.append(nested_root)
                                        decode_count += 1
                                        logger.info(f"  OK Decoded {loaihoso} in xml_content (as XML child)")
                                    except etree.XMLSyntaxError as parse_err:
                                        # If parsing fails, use CDATA section as fallback
                                        logger.warning(f"  ⚠ Cannot parse {loaihoso} as XML, using text: {str(parse_err)}")
                                        noidungfile_elem.text = nested_xml
                                    
                                except Exception as decode_err:
                                    logger.warning(f"  ⚠ Failed to decode {loaihoso}: {str(decode_err)}")
                
                logger.info(f"\nOK Successfully decoded {decode_count} nested XML in xml_content")
        
        # Convert back to string và gán lại vào xml_content
        xml_content = etree.tostring(root_to_modify, encoding='utf-8', xml_declaration=True).decode('utf-8')
        logger.info(f"Final xml_content size: {len(xml_content)} characters")
        # Step 6: Determine overall validation status
        logger.info("\n" + "=" * 80)
        logger.info("VALIDATION SUMMARY")
        logger.info("=" * 80)
        
        is_valid = len(validation_errors) == 0
        
        logger.info(f"Total nested XML validated: {total_validated}")
        logger.info(f"OK Passed: {total_passed}")
        logger.info(f"✗ Failed: {total_failed}")
        logger.info(f"⚠ Warnings: {len(validation_warnings)}")
        
        if validation_errors:
            logger.error(f"\nValidation FAILED with {len(validation_errors)} errors:")
            for error in validation_errors[:10]:  # Show first 10 errors
                ma_lk_info = f", MA_LK: {error.get('ma_lk', 'N/A')}" if error.get('ma_lk') else ''
                logger.error(f"  - HOSO #{error['hoso_index']}{ma_lk_info}, {error['loaihoso']}: {error['error_messages'][0] if error['error_messages'] else 'Unknown error'}")
        else:
            logger.info("\nOK All nested XML validations PASSED")
        
        if validation_warnings:
            logger.warning(f"\nValidation has {len(validation_warnings)} warnings:")
            for warning in validation_warnings[:10]:  # Show first 10 warnings
                logger.warning(f"  - {warning}")
        
        # Step 7: Push validation results to XCom
        logger.info("\nStep 7: Pushing validation results to XCom...")
        
        validation_result = {
            'is_valid': is_valid,
            'validation_status': 'PASSED' if is_valid else 'FAILED',
            'total_validated': total_validated,
            'total_passed': total_passed,
            'total_failed': total_failed,
            'error_count': len(validation_errors),
            'warning_count': len(validation_warnings),
            'errors': validation_errors,
            'warnings': validation_warnings,
            'validation_details': validation_details,
            'validated_at': datetime.utcnow().isoformat()
        }
        
        ti.xcom_push(key='validation_result', value=validation_result)
        
        # Push decoded XML (xml_content đã được decode tất cả nested XML)
        ti.xcom_push(key='validated_xml', value=xml_content)
        logger.info(f"Pushed decoded XML to XCom ({len(xml_content)} characters)")
        
        logger.info("=" * 80)
        logger.info(f"TASK 2: VALIDATE NESTED XML - Completed")
        logger.info(f"Status: {'OK PASSED' if is_valid else '✗ FAILED'}")
        logger.info(f"Validated: {total_validated}, Passed: {total_passed}, Failed: {total_failed}")
        logger.info("=" * 80)
        
        # Nếu có quá nhiều lỗi nghiêm trọng, raise exception
        if not is_valid and len(validation_errors) > 10:
            raise AirflowFailException(f"Validation failed with {len(validation_errors)} nested XML errors")
        
        return validation_result
        
    except Exception as e:
        logger.error(f"TASK 2: VALIDATE NESTED XML - Failed with error: {str(e)}")
        logger.exception(e)
        raise


# ============================================
# TASK 3: TRANSFORM - Transform to DTO
# ============================================

def task_transform_to_dto(**context):
    """
    TASK 3: Transform validated XML sang DTO structure
    
    Input: Validated XML string
    Output: Complete DTO (dict) với tất cả các bảng
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("=" * 80)
    logger.info("TASK 3: TRANSFORM - Starting")
    logger.info("=" * 80)
    
    try:
        # Step 1: Pull validated XML
        logger.info("Step 1: Pulling validated XML from previous task...")
        
        xml_content = ti.xcom_pull(task_ids='validate_xml', key='validated_xml')
        validation_result = ti.xcom_pull(task_ids='validate_xml', key='validation_result')
        file_metadata = ti.xcom_pull(task_ids='load_from_s3', key='file_metadata')
        
        if not xml_content:
            raise ValueError("No validated XML received")
        
        logger.info(f"Validation status: {validation_result['validation_status']}")
        
        # Step 2: Initialize transformer
        logger.info("Step 2: Initializing BHYT transformer...")
        
        transformer = CompleteBHYTTransformer()
        
        # Step 3: Transform XML to DTO
        logger.info("Step 3: Transforming XML to DTO structure...")
        
        output_filename = f"rte.json"
       
        # with open(output_filename, 'w', encoding='utf-8') as f:
        #     json.dump(xml_content, f, ensure_ascii=False, indent=2)
        # logger.info(f"{xml_content}")
        dto = transformer.transform_complete_xml(xml_content)

        # output_filename = f"dto.json"
       
        # with open(output_filename, 'w', encoding='utf-8') as f:
        #     json.dump(dto, f, ensure_ascii=False, indent=2)
        logger.info(f"OK Transformation to DTO completed")
        logger.info(f"{dto}")
        logger.info(f"Transformation completed successfully")
        logger.info(f"DTO resource type: {dto['resourceType']}")
        logger.info(f"DTO standard: {dto['standard']}")
        
        # Step 4: Extract key information
        logger.info("Step 4: Extracting key information...")
        
        tonghop = dto.get('xml1_tonghop', {})
        logger.info(f"{tonghop}")
        if isinstance(tonghop, list) and len(tonghop) > 0:
            tonghop_item = tonghop[0]
        else:
            tonghop_item = {}
        key_info = {
            'MA_LK': tonghop_item.get('MA_LK'),
            'MA_BN': tonghop_item.get('MA_BN'),
            'HO_TEN': tonghop_item.get('HO_TEN'),
            'NGAY_SINH': tonghop_item.get('NGAY_SINH'),
            'GIOI_TINH': tonghop_item.get('GIOI_TINH'),
            'MA_THE': tonghop_item.get('MA_THE_BHYT'),  
            'MA_CSKCB': tonghop_item.get('MA_CSKCB'),
            'NGAY_VAO': tonghop_item.get('NGAY_VAO'),
            'NGAY_RA': tonghop_item.get('NGAY_RA'),
            'T_TONGCHI': tonghop_item.get('T_TONGCHI'),
            'T_BHTT': tonghop_item.get('T_BHTT'),
            'T_BNTT': tonghop_item.get('T_BNTT'),
        }
        
        logger.info("Key information extracted:")
        for key, value in key_info.items():
            logger.info(f"  {key}: {value}")
        
        # Step 5: Record counts
        record_counts = dto['metadata'].get('recordCount', {})
        
        logger.info(f"Record counts after transformation:")
        logger.info(f"  Thuốc: {record_counts.get('thuoc', 0)}")
        logger.info(f"  DVKT: {record_counts.get('dvkt', 0)}")
        logger.info(f"  CLS: {record_counts.get('cls', 0)}")
        logger.info(f"  Diễn biến lâm sàng: {record_counts.get('dbls', 0)}")
        
        # Step 6: Add source metadata
        logger.info("Step 6: Adding source metadata to DTO...")
        
        dto['source_metadata'] = {
            's3_uri': file_metadata.get('s3_uri'),
            'message_id': file_metadata.get('message_id'),
            'received_at': file_metadata.get('received_at'),
            'validation_result': validation_result,
            'transformation_timestamp': datetime.utcnow().isoformat()
        }
        
        # Step 7: Push DTO to XCom
        logger.info("Step 7: Pushing DTO to XCom...")
        
        # Push complete DTO
        ti.xcom_push(key='complete_dto', value=dto)
        
        # Push summary for easier access
        dto_summary = {
            'ma_lk': key_info['MA_LK'],
            'patient_name': key_info['HO_TEN'],
            'ma_the': key_info['MA_THE'],
            'total_cost': key_info['T_TONGCHI'],
            'record_counts': record_counts,
            'transform_status': 'success'
        }
        
        ti.xcom_push(key='dto_summary', value=dto_summary)
        
        logger.info("=" * 80)
        logger.info("TASK 3: TRANSFORM - Completed successfully")
        logger.info(f"MA_LK: {key_info['MA_LK']}")
        logger.info(f"Patient: {key_info['HO_TEN']}")
        logger.info(f"Total records: {sum(record_counts.values())}")
        logger.info("=" * 80)
        
        return dto_summary
        
    except Exception as e:
        logger.error(f"TASK 3: TRANSFORM - Failed with error: {str(e)}")
        logger.exception(e)
        raise


# ============================================
# TASK 4: VERIFY - Verify với External System
# ============================================

def task_verify_external_system(**context):
    """
    TASK 4: Verify DTO với external systems và data integrity
    
    Input: Complete DTO từ Task 3, Validation results từ Task 2
    Output: Verification results từ external systems và integrity checks
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("=" * 80)
    logger.info("TASK 4: VERIFY - Starting external system verification")
    logger.info("=" * 80)
    
    try:
        # Step 1: Pull DTO and validation results from previous tasks
        logger.info("Step 1: Pulling data from previous tasks...")
        
        dto = ti.xcom_pull(task_ids='transform_to_dto', key='complete_dto')
        dto_summary = ti.xcom_pull(task_ids='transform_to_dto', key='dto_summary')
        validation_result = ti.xcom_pull(task_ids='validate_xml', key='validation_result')
        
        if not dto:
            raise ValueError("No DTO received from transform task")
        
        # Extract tonghop correctly - DTO structure uses 'xml1_tonghop' and it's a list
        xml1_tonghop_list = dto.get('xml1_tonghop', [])
        if isinstance(xml1_tonghop_list, list) and len(xml1_tonghop_list) > 0:
            tonghop = xml1_tonghop_list[0]
        else:
            tonghop = {}
        
        logger.info(f"DTO received: MA_LK = {tonghop.get('MA_LK')}")
        logger.info(f"Patient: {tonghop.get('HO_TEN')}")
        logger.info(f"Validation status from Task 2: {validation_result.get('validation_status')}")
        logger.info(f"Total validated nested XML: {validation_result.get('total_validated', 0)}")
        
        # Step 2: Data Integrity Check
        logger.info("\nStep 2: Performing data integrity checks...")
        
        integrity_checks = {
            'ma_lk_present': bool(tonghop.get('MA_LK')),
            'patient_info_complete': all([
                tonghop.get('HO_TEN'),
                tonghop.get('NGAY_SINH'),
                tonghop.get('MA_THE_BHYT')
            ]),
            'cost_data_present': all([
                tonghop.get('T_TONGCHI') is not None,
                tonghop.get('T_BHTT') is not None,
                tonghop.get('T_BNTT') is not None
            ]),
            'detail_records_present': any([
                len(dto.get('xml2_chitiet_thuoc', [])) > 0,
                len(dto.get('xml3_chitiet_dvkt', [])) > 0,
                len(dto.get('xml4_chitiet_cls', [])) > 0
            ]),
            'xml_validation_passed': validation_result.get('is_valid', False)
        }
        
        integrity_score = sum(integrity_checks.values()) / len(integrity_checks) * 100
        logger.info(f"Data integrity score: {integrity_score:.1f}%")
        
        for check_name, result in integrity_checks.items():
            status = "OK" if result else "✗"
            logger.info(f"  {status} {check_name}: {'PASS' if result else 'FAIL'}")
        
        # Step 3: Verify Patient Identity (MOCK)
        logger.info("\nStep 3: Verifying patient identity with National ID System (MOCK)...")
        
        patient_verification = verify_patient_identity_mock(
            ma_bn=tonghop.get('MA_BN'),
            ho_ten=tonghop.get('HO_TEN'),
            ngay_sinh=tonghop.get('NGAY_SINH'),
            so_cccd=tonghop.get('SO_CCCD')
        )
        
        logger.info(f"Patient verification: {patient_verification['status']}")
        logger.info(f"Match score: {patient_verification['match_score']:.2f}")
        
        # Step 4: Verify BHYT Card (MOCK)
        logger.info("\nStep 4: Verifying BHYT card with BHXH Portal (MOCK)...")
        
        card_verification = verify_bhyt_card_mock(
            ma_the=tonghop.get('MA_THE'),
            ho_ten=tonghop.get('HO_TEN'),
            ngay_sinh=tonghop.get('NGAY_SINH'),
            gt_the_tu=tonghop.get('GT_THE_TU'),
            gt_the_den=tonghop.get('GT_THE_DEN')
        )
        
        logger.info(f"Card verification: {card_verification['status']}")
        logger.info(f"Card is {'VALID' if card_verification['is_valid'] else 'INVALID'}")
        logger.info(f"Coverage: {card_verification['coverage_info']['coverage_percentage']}%")
        
        # Step 5: Verify Facility Registration (MOCK)
        logger.info("\nStep 5: Verifying facility registration (MOCK)...")
        
        facility_verification = verify_facility_mock(
            ma_cskcb=tonghop.get('MA_CSKCB'),
            ma_khoa=tonghop.get('MA_KHOA')
        )
        
        logger.info(f"Facility verification: {facility_verification['status']}")
        logger.info(f"Facility name: {facility_verification['facility_info']['facility_name']}")
        
        # Step 6: Verify Medication Codes (MOCK - Sample only)
        logger.info("\nStep 6: Verifying medication codes (MOCK - sample)...")
        
        thuoc_list = dto.get('xml2_chitiet_thuoc', [])
        medication_verification = verify_medications_mock(thuoc_list[:5])  # Sample first 5
        
        logger.info(f"Verified {len(medication_verification['verified_items'])} medications")
        logger.info(f"Valid: {medication_verification['valid_count']}, "
                   f"Invalid: {medication_verification['invalid_count']}")
        
        # Step 7: Verify Service Codes (MOCK - Sample only)
        logger.info("\nStep 7: Verifying service codes (MOCK - sample)...")
        
        dvkt_list = dto.get('xml3_chitiet_dvkt', [])
        service_verification = verify_services_mock(dvkt_list[:5])  # Sample first 5
        
        logger.info(f"Verified {len(service_verification['verified_items'])} services")
        
        # Step 8: Cross-check Cost Calculation (MOCK)
        logger.info("\nStep 8: Cross-checking cost calculation...")
        
        cost_verification = verify_cost_calculation_mock(
            t_tongchi=tonghop.get('T_TONGCHI'),
            t_bhtt=tonghop.get('T_BHTT'),
            t_bntt=tonghop.get('T_BNTT'),
            t_bncct=tonghop.get('T_BNCCT'),
            detail_records={
                'thuoc': thuoc_list,
                'dvkt': dvkt_list
            }
        )
        
        logger.info(f"Cost verification: {cost_verification['status']}")
        logger.info(f"Discrepancy: {cost_verification['discrepancy_amount']:.2f} VND")
        
        # Step 9: Compile verification results
        logger.info("\nStep 9: Compiling verification results...")
        
        verification_results = {
            'overall_status': 'VERIFIED',
            'verification_timestamp': datetime.utcnow().isoformat(),
            'ma_lk': tonghop.get('MA_LK'),
            
            # Data integrity
            'data_integrity': {
                'integrity_score': integrity_score,
                'checks': integrity_checks,
                'xml_validation_summary': {
                    'total_validated': validation_result.get('total_validated', 0),
                    'total_passed': validation_result.get('total_passed', 0),
                    'total_failed': validation_result.get('total_failed', 0),
                    'status': validation_result.get('validation_status', 'UNKNOWN')
                }
            },
            
            # External verifications
            'verifications': {
                'patient_identity': patient_verification,
                'bhyt_card': card_verification,
                'facility': facility_verification,
                'medications': medication_verification,
                'services': service_verification,
                'cost_calculation': cost_verification
            },
            
            'verification_summary': {
                'total_checks': 6,
                'passed_checks': 0,
                'failed_checks': 0,
                'warning_checks': 0
            }
        }
        
        # Count verification results
        for check_name, check_result in verification_results['verifications'].items():
            status = check_result.get('status', 'unknown')
            if status in ['verified', 'valid', 'passed']:
                verification_results['verification_summary']['passed_checks'] += 1
            elif status in ['failed', 'invalid']:
                verification_results['verification_summary']['failed_checks'] += 1
            else:
                verification_results['verification_summary']['warning_checks'] += 1
        
        # Determine overall status
        if verification_results['verification_summary']['failed_checks'] > 0:
            verification_results['overall_status'] = 'FAILED'
        elif verification_results['verification_summary']['warning_checks'] > 2:
            verification_results['overall_status'] = 'WARNING'
        
        # Step 9: Push verification results to XCom
        logger.info("Step 9: Pushing verification results to XCom...")
        
        ti.xcom_push(key='verification_results', value=verification_results)
        
        logger.info("=" * 80)
        logger.info("TASK 4: VERIFY - Completed")
        logger.info(f"Overall Status: {verification_results['overall_status']}")
        logger.info(f"Passed: {verification_results['verification_summary']['passed_checks']}/{verification_results['verification_summary']['total_checks']}")
        logger.info("=" * 80)
        
        return verification_results
        
    except Exception as e:
        logger.error(f"TASK 4: VERIFY - Failed with error: {str(e)}")
        logger.exception(e)
        raise


# ============================================
# TASK 5: OUTPUT JSON - Generate và Send to API
# ============================================

def task_output_json(**context):
    """
    TASK 5: Generate final JSON output và gửi đến API endpoint
    
    Input: Complete DTO + Verification Results
    Output: Final JSON file và API response
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("=" * 80)
    logger.info("TASK 5: OUTPUT JSON - Starting")
    logger.info("=" * 80)
    
    try:
        # Step 1: Pull all data from previous tasks
        logger.info("Step 1: Pulling data from all previous tasks...")
        
        file_metadata = ti.xcom_pull(task_ids='load_from_s3', key='file_metadata')
        validation_result = ti.xcom_pull(task_ids='validate_xml', key='validation_result')
        dto = ti.xcom_pull(task_ids='transform_to_dto', key='complete_dto')


        verification_results = ti.xcom_pull(task_ids='verify_external', key='verification_results')
        
        # Step 2: Build final JSON structure
        logger.info("Step 2: Building final JSON structure...")
        
        # Determine overall status for each record
        has_validation_errors = validation_result.get('error_count', 0) > 0
        
        # Process each record to add status and errors
        records = dto.get('records', [])
        processed_data = []
        
        for idx, record in enumerate(records):
            # Find validation errors for this record (hoso_index)
            record_errors = [
                err for err in validation_result.get('errors', [])
                if err.get('hoso_index') == idx + 1
            ]
            
            # Add status and errors to record
            record_with_status = {
                'MA_LK': record.get('TONG_HOP', {}).get('MA_LK', ''),
                'TrangThai': 'Error' if record_errors else 'Success',
                'Errors': [
                    {
                        'LOAIHOSO': err.get('loaihoso'),
                        'XSD_FILE': err.get('xsd_file'),
                        'ERROR_MESSAGES': err.get('error_messages', [])
                    }
                    for err in record_errors
                ],
                'TONG_HOP': record.get('TONG_HOP', {}),
                'DSACH_CHI_TIET_THUOC': record.get('DSACH_CHI_TIET_THUOC', []),
                'DSACH_CHI_TIET_DVKT': record.get('DSACH_CHI_TIET_DVKT', []),
                'DSACH_CHI_TIET_CLS': record.get('DSACH_CHI_TIET_CLS', []),
                'DSACH_CHI_TIET_DIEN_BIEN_BENH': record.get('DSACH_CHI_TIET_DIEN_BIEN_BENH', []),
                'DSACH_HO_SO_HIV_AIDS': record.get('DSACH_HO_SO_HIV_AIDS', []),
                'GIAY_RA_VIEN': record.get('GIAY_RA_VIEN', {}),
                'TOM_TAT_HO_SO_BA': record.get('TOM_TAT_HO_SO_BA', {}),
                'GIAY_CHUNG_SINH': record.get('GIAY_CHUNG_SINH', {}),
                'GIAY_NGHI_DUONG_THAI': record.get('GIAY_NGHI_DUONG_THAI', {}),
                'GIAY_NGHI_VIEC_HUONG_BHXH': record.get('GIAY_NGHI_VIEC_HUONG_BHXH', {}),
                'DSACH_GIAM_DINH_Y_KHOA': record.get('DSACH_GIAM_DINH_Y_KHOA', []),
                'DSACH_GIAY_CHUYEN_TUYEN': record.get('DSACH_GIAY_CHUYEN_TUYEN', []),
                'DSACH_GIAY_HEN_KHAM_LAI': record.get('DSACH_GIAY_HEN_KHAM_LAI', []),
                'DSACH_THONG_TIN_LAO': record.get('DSACH_THONG_TIN_LAO', [])
            }
            processed_data.append(record_with_status)
        
        # Count records with errors
        records_with_errors = sum(1 for r in processed_data if r['TrangThai'] == 'Error')
        
        # Build final JSON with new structure
        final_json = {
            'header': {
                'MACSKCB': dto.get('header', {}).get('MACSKCB', ''),
                'NGAYLAP': dto.get('header', {}).get('NGAYLAP', ''),
                'SoLuongHoSo': len(records),
                'SoLuongHoSoLoi': records_with_errors,
                'message_id': file_metadata.get('message_id', '')
            },
            'data': processed_data
        }
        
        # Step 3: Generate JSON string
        logger.info("Step 3: Generating JSON string...")
        
        json_string = json.dumps(final_json, ensure_ascii=False, indent=2)
        json_size = len(json_string)
        
        logger.info(f"JSON generated successfully. Size: {json_size:,} bytes")
        logger.info(f"Header: MACSKCB={final_json['header']['MACSKCB']}, Records={final_json['header']['SoLuongHoSo']}, Errors={final_json['header']['SoLuongHoSoLoi']}")
        
        # Step 4: Save JSON to local file (temporary)
        logger.info("Step 4: Saving JSON to local file...")
        
        ma_lk = processed_data[0].get('MA_LK', 'unknown') if processed_data else 'unknown'
        output_filename = f"bhyt_{ma_lk}_{context['execution_date'].strftime('%Y%m%d_%H%M%S')}.json"
        output_path = f"/tmp/{output_filename}"
        temp_dir = tempfile.gettempdir()
        output_path = os.path.join(temp_dir, output_filename)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(json_string, f, ensure_ascii=False, indent=2)
        # with open(output_path, 'w', encoding='utf-8') as f:
        #     f.write(json_string)
        
        logger.info(f"JSON saved to: {output_path}")
        
        # Step 5: Send JSON to API endpoint
        logger.info("Step 5: Sending JSON to API endpoint...")
        
        api_endpoint = API_CONFIG['endpoint']
        logger.info(f"API endpoint: {api_endpoint}")
        
        api_response = send_json_to_api(
            endpoint_url=api_endpoint,
            json_data=json_string,
            ma_lk=ma_lk,
            logger=logger
        )
        
        logger.info(f"API call status: {api_response['status']}")
        logger.info(f"API response code: {api_response['status_code']}")
        
        if api_response['status'] != 'success':
            logger.warning(f"API call failed: {api_response.get('error_message')}")
        
        # Step 6: Upload JSON to S3 output bucket (backup)
        logger.info("Step 6: Uploading JSON to S3 output bucket (backup)...")
        
        try:
            import boto3
            
            s3_client = boto3.client('s3',
                aws_access_key_id=Variable.get('aws_access_key_id', default_var=None),
                aws_secret_access_key=Variable.get('aws_secret_access_key', default_var=None),
                region_name=Variable.get('aws_region', default_var='ap-southeast-1')
            )
            
            output_bucket = Variable.get('bhyt_output_bucket', 'bhyt-processed-json')
            output_key = f"processed/{context['execution_date'].strftime('%Y/%m/%d')}/{output_filename}"
            
            s3_client.upload_file(
                output_path,
                output_bucket,
                output_key,
                ExtraArgs={
                    'ContentType': 'application/json',
                    'Metadata': {
                        'ma_lk': ma_lk,
                        'processing_date': context['execution_date'].isoformat(),
                        'pipeline_version': '2.0.0',
                        'api_status': api_response['status']
                    }
                }
            )
            
            output_s3_uri = f"s3://{output_bucket}/{output_key}"
            logger.info(f"JSON uploaded to S3: {output_s3_uri}")
            
        except Exception as s3_error:
            logger.warning(f"S3 upload failed (non-critical): {str(s3_error)}")
            output_s3_uri = None
        
        # Step 7: Push final results to XCom
        logger.info("Step 7: Pushing final results to XCom...")
        
        final_results = {
            'output_s3_uri': output_s3_uri,
            'output_filename': output_filename,
            'output_local_path': output_path,
            'json_size': json_size,
            'ma_lk': ma_lk,
            'macskcb': final_json['header']['MACSKCB'],
            'total_records': final_json['header']['SoLuongHoSo'],
            'records_with_errors': final_json['header']['SoLuongHoSoLoi'],
            'api_response': api_response,
            'api_endpoint': api_endpoint,
            'completed_at': datetime.utcnow().isoformat()
        }
        
        ti.xcom_push(key='final_results', value=final_results)
        
        # Clean up temp file
        try:
            # os.remove(output_path)
            logger.info(f"Cleaned up temporary file: {output_path}")
        except Exception as e:
            logger.warning(f"Failed to clean up temp file: {str(e)}")
        
        logger.info("=" * 80)
        logger.info("TASK 5: OUTPUT JSON - Completed successfully")
        logger.info(f"Output S3: {output_s3_uri}")
        logger.info(f"API Status: {api_response['status']}")
        logger.info(f"MACSKCB: {final_json['header']['MACSKCB']}")
        logger.info(f"Total Records: {final_json['header']['SoLuongHoSo']}")
        logger.info(f"Records with Errors: {final_json['header']['SoLuongHoSoLoi']}")
        logger.info(f"MA_LK: {ma_lk}")
        logger.info("=" * 80)
        
        return final_results
        
    except Exception as e:
        logger.error(f"TASK 5: OUTPUT JSON - Failed with error: {str(e)}")
        logger.exception(e)
        raise


# ============================================
# API Helper Functions
# ============================================

def send_json_to_api(endpoint_url: str, json_data: str, ma_lk: str, logger) -> dict:
    """
    Send JSON data to ASP.NET Core API endpoint
    
    Args:
        endpoint_url: API endpoint URL (e.g., https://localhost:44315/api/app/files/test)
        json_data: JSON string to send
        ma_lk: MA_LK for tracking
        logger: Logger instance
        
    Returns:
        dict with status, status_code, response_data, error_message
    """
    import requests
    import urllib3
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    
    logger.info(f"Preparing to send JSON to API: {endpoint_url}")
    logger.info(f"JSON size: {len(json_data)} bytes")
    logger.info(f"MA_LK: {ma_lk}")
    
    # Disable SSL warnings for localhost (development only)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Setup retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Prepare request
    headers = {
        # 'Content-Type': 'application/json',
        'Accept': 'application/json',
        'User-Agent': 'Airflow-BHYT-ETL/2.0',
        'X-MA-LK': ma_lk,
        'X-Request-ID': f"req_{int(datetime.utcnow().timestamp())}_{ma_lk}"
    }
    
    # Prepare payload - API endpoint expects parameter "xml" with JSON string value
    # For ASP.NET Core, we can send as form data or JSON body
    
    # Option 1: Send as form data (recommended for string parameter)
    payload = {
        'xml': json_data
    }
    
    logger.info("Sending request to API...")
    logger.info(f"Headers: {json.dumps({k: v for k, v in headers.items() if k != 'Authorization'}, indent=2)}")
    
    max_retries = 3
    retry_count = 0
    last_error = None
    
    while retry_count < max_retries:
        try:
            # Make POST request
            response = session.post(
                endpoint_url,
                data=payload,  # Send as form data
                headers=headers,
                timeout=60,  # 60 seconds timeout
                verify=False  # Skip SSL verification for localhost (DEVELOPMENT ONLY!)
            )
            
            logger.info(f"API Response Status: {response.status_code}")
            logger.info(f"API Response Headers: {dict(response.headers)}")
            
            # Log response body (first 500 chars)
            response_text = response.text[:500] if response.text else ''
            logger.info(f"API Response Body (preview): {response_text}")
            
            # Check if request was successful
            if response.status_code in [200, 201, 202]:
                logger.info(f"OK API call successful (status {response.status_code})")
                
                # Try to parse JSON response
                try:
                    response_data = response.json()
                except:
                    response_data = {'raw_response': response.text}
                
                return {
                    'status': 'success',
                    'status_code': response.status_code,
                    'response_data': response_data,
                    'response_text': response.text,
                    'request_id': headers.get('X-Request-ID'),
                    'retry_count': retry_count,
                    'timestamp': datetime.utcnow().isoformat()
                }
            
            elif response.status_code in [400, 422]:
                # Client error - don't retry
                logger.error(f"✗ API call failed with client error {response.status_code}")
                logger.error(f"Response: {response.text}")
                
                return {
                    'status': 'failed',
                    'status_code': response.status_code,
                    'error_message': f"Client error: {response.text}",
                    'response_text': response.text,
                    'retry_count': retry_count,
                    'timestamp': datetime.utcnow().isoformat()
                }
            
            elif response.status_code in [500, 502, 503, 504]:
                # Server error - retry
                logger.warning(f"⚠ API call failed with server error {response.status_code}, retrying...")
                last_error = f"Server error {response.status_code}: {response.text}"
                retry_count += 1
                
                if retry_count < max_retries:
                    import time
                    wait_time = 2 ** retry_count  # Exponential backoff
                    logger.info(f"Waiting {wait_time} seconds before retry {retry_count + 1}/{max_retries}...")
                    time.sleep(wait_time)
                    continue
            
            else:
                # Other errors
                logger.error(f"✗ API call failed with status {response.status_code}")
                logger.error(f"Response: {response.text}")
                
                return {
                    'status': 'failed',
                    'status_code': response.status_code,
                    'error_message': f"Unexpected status code: {response.text}",
                    'response_text': response.text,
                    'retry_count': retry_count,
                    'timestamp': datetime.utcnow().isoformat()
                }
                
        except requests.exceptions.Timeout as e:
            logger.error(f"✗ API call timeout (attempt {retry_count + 1}/{max_retries})")
            last_error = f"Timeout: {str(e)}"
            retry_count += 1
            
            if retry_count < max_retries:
                import time
                time.sleep(2 ** retry_count)
                continue
        
        except requests.exceptions.ConnectionError as e:
            logger.error(f"✗ Connection error (attempt {retry_count + 1}/{max_retries}): {str(e)}")
            last_error = f"Connection error: {str(e)}"
            retry_count += 1
            
            if retry_count < max_retries:
                import time
                time.sleep(2 ** retry_count)
                continue
        
        except Exception as e:
            logger.error(f"✗ Unexpected error during API call: {str(e)}")
            logger.exception(e)
            last_error = str(e)
            break
    
    # All retries failed
    logger.error(f"✗ API call failed after {max_retries} retries")
    
    return {
        'status': 'failed',
        'status_code': 0,
        'error_message': f"Failed after {max_retries} retries. Last error: {last_error}",
        'retry_count': retry_count,
        'timestamp': datetime.utcnow().isoformat()
    }


def send_json_to_api_alternative(endpoint_url: str, json_data: str, ma_lk: str, logger) -> dict:
    """
    Alternative method: Send JSON in request body với Content-Type application/json
    Nếu API endpoint có thể accept JSON body thay vì form data
    """
    import requests
    import urllib3
    
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    logger.info(f"Sending JSON to API (alternative method): {endpoint_url}")
    
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'User-Agent': 'Airflow-BHYT-ETL/2.0',
        'X-MA-LK': ma_lk
    }
    
    # Send entire JSON as request body
    # API sẽ cần [FromBody] attribute thay vì simple string parameter
    try:
        response = requests.post(
            endpoint_url,
            data=json_data.encode('utf-8'),  # Send raw JSON string
            headers=headers,
            timeout=60,
            verify=False
        )
        
        if response.status_code in [200, 201, 202]:
            return {
                'status': 'success',
                'status_code': response.status_code,
                'response_data': response.text,
                'timestamp': datetime.utcnow().isoformat()
            }
        else:
            return {
                'status': 'failed',
                'status_code': response.status_code,
                'error_message': response.text,
                'timestamp': datetime.utcnow().isoformat()
            }
            
    except Exception as e:
        logger.error(f"API call failed: {str(e)}")
        return {
            'status': 'error',
            'status_code': 0,
            'error_message': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }



# ============================================
# MOCK VERIFICATION FUNCTIONS
# ============================================

def verify_patient_identity_mock(ma_bn: str, ho_ten: str, ngay_sinh: str, so_cccd: str) -> dict:
    """Mock patient identity verification"""
    import random
    
    return {
        'status': 'verified',
        'match_score': random.uniform(0.85, 1.0),
        'verification_method': 'CCCD_LOOKUP',
        'verified_info': {
            'ma_bn': ma_bn,
            'ho_ten': ho_ten,
            'ngay_sinh': ngay_sinh,
            'so_cccd': so_cccd,
            'dia_chi_xac_thuc': '123 Đường ABC, Quận 1, TP.HCM'
        },
        'verified_at': datetime.utcnow().isoformat(),
        'verification_source': 'National ID Database (MOCK)'
    }

def verify_bhyt_card_mock(ma_the: str, ho_ten: str, ngay_sinh: str, 
                          gt_the_tu: str, gt_the_den: str) -> dict:
    """Mock BHYT card verification"""
    import random
    
    is_valid = random.choice([True, True, True, False])  # 75% valid
    
    return {
        'status': 'valid' if is_valid else 'invalid',
        'is_valid': is_valid,
        'ma_the': ma_the,
        'card_info': {
            'ho_ten': ho_ten,
            'ngay_sinh': ngay_sinh,
            'ma_dkbd': 'CS01234',
            'ma_kv': 'K1',
            'gia_tri_tu': gt_the_tu,
            'gia_tri_den': gt_the_den,
            'loai_the': 'TN1'
        },
        'coverage_info': {
            'coverage_percentage': 100 if is_valid else 0,
            'is_active': is_valid,
            'co_payment_rate': 0.05
        },
        'verified_at': datetime.utcnow().isoformat(),
        'verification_source': 'BHXH Portal (MOCK)'
    }

def verify_facility_mock(ma_cskcb: str, ma_khoa: str) -> dict:
    """Mock facility verification"""
    
    return {
        'status': 'verified',
        'ma_cskcb': ma_cskcb,
        'facility_info': {
            'facility_name': 'Bệnh viện Đa khoa Trung ương',
            'facility_type': 'Tuyến Trung ương',
            'facility_level': 'Hạng I',
            'address': '123 Đường Giải Phóng, Hà Nội',
            'license_number': 'BV-001234',
            'is_active': True
        },
        'department_info': {
            'ma_khoa': ma_khoa,
            'ten_khoa': 'Khoa Nội Tổng Hợp',
            'is_active': True
        },
        'verified_at': datetime.utcnow().isoformat(),
        'verification_source': 'Ministry of Health Database (MOCK)'
    }

def verify_medications_mock(thuoc_list: list) -> dict:
    """Mock medication verification"""
    import random
    
    verified_items = []
    valid_count = 0
    invalid_count = 0
    
    for thuoc in thuoc_list[:10]:  # Verify first 10
        is_valid = random.choice([True, True, True, False])  # 75% valid
        
        verified_items.append({
            'ma_thuoc': thuoc.get('MA_THUOC'),
            'ten_thuoc': thuoc.get('TEN_THUOC'),
            'is_valid': is_valid,
            'is_in_catalog': is_valid,
            'unit_price_verified': is_valid,
            'insurance_covered': is_valid
        })
        
        if is_valid:
            valid_count += 1
        else:
            invalid_count += 1
    
    return {
        'status': 'completed',
        'verified_items': verified_items,
        'total_checked': len(verified_items),
        'valid_count': valid_count,
        'invalid_count': invalid_count,
        'verification_source': 'National Drug Catalog (MOCK)'
    }

def verify_services_mock(dvkt_list: list) -> dict:
    """Mock service code verification"""
    import random
    
    verified_items = []
    
    for dvkt in dvkt_list[:10]:  # Verify first 10
        is_valid = random.choice([True, True, True, False])
        
        verified_items.append({
            'ma_dich_vu': dvkt.get('MA_DICH_VU'),
            'ten_dich_vu': dvkt.get('TEN_DICH_VU'),
            'is_valid': is_valid,
            'price_verified': is_valid
        })
    
    return {
        'status': 'completed',
        'verified_items': verified_items,
        'verification_source': 'Service Catalog (MOCK)'
    }

def verify_cost_calculation_mock(t_tongchi: float, t_bhtt: float, 
                                 t_bntt: float, t_bncct: float, 
                                 detail_records: dict) -> dict:
    """Mock cost calculation verification"""
    import random
    
    # Handle None values
    if t_tongchi is None or t_tongchi == 0:
        return {
            'status': 'warning',
            'declared_total': t_tongchi,
            'calculated_total': 0,
            'discrepancy_amount': 0,
            'discrepancy_percentage': 0,
            'cost_breakdown_verified': False,
            'verification_source': 'Cost Verification Engine (MOCK)',
            'warning_message': 'Total cost (T_TONGCHI) is None or zero'
        }
    
    # Calculate from details (simplified)
    calculated_total = t_tongchi * random.uniform(0.98, 1.02)
    discrepancy = abs(t_tongchi - calculated_total)
    
    return {
        'status': 'passed' if discrepancy < 1000 else 'warning',
        'declared_total': t_tongchi,
        'calculated_total': calculated_total,
        'discrepancy_amount': discrepancy,
        'discrepancy_percentage': (discrepancy / t_tongchi * 100) if t_tongchi > 0 else 0,
        'cost_breakdown_verified': True,
        'verification_source': 'Cost Verification Engine (MOCK)'
    }

def call_external_api_to_save(data: dict, ma_lk: str) -> dict:
    """Mock API call to save data"""
    import random
    import time
    
    time.sleep(0.5)  # Simulate API delay
    
    return {
        'status': 'success',
        'record_id': f"REC_{ma_lk}_{int(time.time())}",
        'api_endpoint': 'https://api.example.com/bhyt/records',
        'response_code': 201,
        'message': 'Record saved successfully',
        'saved_at': datetime.utcnow().isoformat()
    }


# ============================================
# Define Airflow Tasks
# ============================================

task_1_load = PythonOperator(
    task_id='load_from_s3',
    python_callable=task_load_from_s3,
    provide_context=True,
    dag=dag,
)

task_2_validate = PythonOperator(
    task_id='validate_xml',
    python_callable=task_validate_xml,
    provide_context=True,
    dag=dag,
)

task_3_transform = PythonOperator(
    task_id='transform_to_dto',
    python_callable=task_transform_to_dto,
    provide_context=True,
    dag=dag,
)

task_4_verify = PythonOperator(
    task_id='verify_external',
    python_callable=task_verify_external_system,
    provide_context=True,
    dag=dag,
)

task_5_output = PythonOperator(
    task_id='output_json',
    python_callable=task_output_json,
    provide_context=True,
    dag=dag,
)


# ============================================
# Set Task Dependencies
# ============================================

task_1_load >> task_2_validate >> task_3_transform >> task_4_verify >> task_5_output

if __name__ == "__main__":
    dag.test()
