"""
Complete BHYT ETL Pipeline với luồng:
Load -> Validate -> Transform -> Verify -> Output JSON
"""

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
sys.path.insert(0, '/opt/airflow/plugins/helpers')

from s3_loader import S3FileLoader
from bhyt_transformer_complete import CompleteBHYTTransformer
from bhyt_validator import CompleteBHYTValidator
from external_verifier import ExternalSystemVerifier

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
    TASK 1: Load message từ SQS, download file từ S3, decode XML
    
    Input: SQS Message với S3 path
    Output: Raw XML string (đã decode)
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    execution_date = context['execution_date']
    
    logger.info("=" * 80)
    logger.info(f"TASK 1: LOAD - Starting at {execution_date}")
    logger.info("=" * 80)
    
    try:
        # Step 1: Receive message từ SQS Queue
        logger.info("Step 1: Receiving message from SQS...")
        
        # sqs_hook = SqsHook(aws_conn_id='aws_default')
        # queue_url = Variable.get('bhyt_sqs_queue_url', 
        #                          'https://sqs.ap-southeast-1.amazonaws.com/123456789/bhyt-xml-queue')
        
        # messages = sqs_hook.receive_message(
        #     queue_url=queue_url,
        #     max_number_of_messages=1,
        #     wait_time_seconds=10,
        #     attribute_names=['All']
        # )
        
        # if not messages or 'Messages' not in messages:
        #     logger.warning("No messages available in queue")
        #     raise AirflowFailException("No messages to process")
        
        # message = messages['Messages'][0]
        # message_body = json.loads(message['Body'])
        # receipt_handle = message['ReceiptHandle']
        
        # logger.info(f"Message received: {message['MessageId']}")
        
        # # Step 2: Extract S3 information từ message
        # logger.info("Step 2: Extracting S3 information...")
        
        # # Message format có thể là:
        # # {"bucket": "bhyt-xml-bucket", "key": "2025/10/28/file.xml"}
        # # hoặc {"Records": [{"s3": {"bucket": {"name": "..."}, "object": {"key": "..."}}}]}
        
        # if 'Records' in message_body:
        #     # S3 Event Notification format
        #     s3_record = message_body['Records'][0]['s3']
        #     s3_bucket = s3_record['bucket']['name']
        #     s3_key = s3_record['object']['key']
        # else:
        #     # Simple format
        #     s3_bucket = message_body.get('bucket')
        #     s3_key = message_body.get('key')
        
        # if not s3_bucket or not s3_key:
        #     raise ValueError("Missing S3 bucket or key in message")
        
        # logger.info(f"S3 Location: s3://{s3_bucket}/{s3_key}")
        
        # Step 3: Download file từ S3
        logger.info("Step 3: Downloading file from S3...")
        
        s3_loader = S3FileLoader(
            aws_access_key=Variable.get('aws_access_key_id', default_var=None),
            aws_secret_key=Variable.get('aws_secret_access_key', default_var=None),
            region_name=Variable.get('aws_region', default_var='ap-southeast-1')
        )
        
        # Download và auto-detect encoding/compression
        file_path = Path(__file__).parent / "test.xml"
        with open(file_path, "rb") as f:
            file_content = f.read()
        # xml_content = s3_loader.download_and_decode_file(s3_bucket, s3_key)
        loader = S3FileLoader(
            aws_access_key="fake",
            aws_secret_key="fake",
            region_name="ap-southeast-1"    
        )
        xml_content = loader._detect_and_decode(file_content, filename="test.xml")
        
        logger.info(f"Successfully downloaded and decoded XML")
        logger.info(f"XML content length: {len(xml_content)} characters")
        logger.info(xml_content)
        output_filename = f"loaded_xml_{execution_date.strftime('%Y%m%dT%H%M%S')}.json"
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(xml_content, f, ensure_ascii=False, indent=2)
        # Step 4: Basic XML structure check
        logger.info("Step 4: Performing basic XML structure check...")
        
        if not xml_content.strip():
            raise ValueError("Empty XML content")
        
        if not ('<' in xml_content and '>' in xml_content):
            raise ValueError("Invalid XML structure - missing angle brackets")
        
        # Check for common XML elements
        required_elements = ['<?xml', '<HOSO', '<TONG_HOP', 'MA_LK']
        found_elements = [elem for elem in required_elements if elem in xml_content]
        
        logger.info(f"Found XML elements: {found_elements}")
        
        # Step 5: Push data to XCom
        logger.info("Step 5: Pushing data to XCom...")
        
        # Push raw XML content
        ti.xcom_push(key='raw_xml_content', value=xml_content)
        
        # Push metadata for test context
        metadata = {
            'message_id': 'test-message-id',
            's3_bucket': 'test-bucket',
            's3_key': 'test/file.xml',
            's3_uri': 'test://test-bucket/test/file.xml',
            'file_size': len(xml_content),
            'sqs_receipt_handle': None,
            'received_at': datetime.utcnow().isoformat(),
            'message_attributes': {},
            'test_mode': True
        }
        
        ti.xcom_push(key='file_metadata', value=metadata)
        
        logger.info("=" * 80)
        logger.info("TASK 1: LOAD - Completed successfully")
        logger.info(f"Metadata: {json.dumps(metadata, indent=2)}")
        logger.info("=" * 80)
        
        return {
            'status': 'success',
            'file_size': len(xml_content),
            's3_uri': metadata['s3_uri']
        }
        
    except Exception as e:
        logger.error(f"TASK 1: LOAD - Failed with error: {str(e)}")
        logger.exception(e)
        raise AirflowFailException(f"Load task failed: {str(e)}")


# ============================================
# TASK 2: VALIDATE - Validate XML Structure
# ============================================

def task_validate_xml(**context):
    """
    TASK 2: Validate XML structure và business rules
    
    Input: Raw XML string
    Output: Validation result và parsed XML root

    TASK 2: Validate XML thực tế (không dùng XSD chi tiết)
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("=" * 80)
    logger.info("TASK 2: VALIDATE - Starting")
    logger.info("=" * 80)
    
    try:
        # Step 1: Pull XML content
        logger.info("Step 1: Pulling XML content from previous task...")
        
        xml_content = ti.xcom_pull(task_ids='load_from_s3', key='raw_xml_content')
        file_metadata = ti.xcom_pull(task_ids='load_from_s3', key='file_metadata')
        
        if not xml_content:
            raise ValueError("No XML content received from previous task")
        
        logger.info(f"XML content size: {len(xml_content)} characters")
        
        # Step 2: XML Syntax Validation (wrapper only)
        logger.info("Step 2: Validating wrapper XML syntax...")
        
        import xml.etree.ElementTree as ET
        
        try:
            # Remove BOM if exists
            if xml_content.startswith('\ufeff'):
                xml_content = xml_content[1:]
                logger.info("Removed BOM from XML content")
            
            # Parse wrapper XML
            root = ET.fromstring(xml_content.encode('utf-8'))
            logger.info(f"✓ Wrapper XML parsed successfully. Root element: {root.tag}")
            
        except ET.ParseError as e:
            logger.error(f"XML parsing error: {str(e)}")
            raise ValueError(f"Invalid XML syntax: {str(e)}")
        
        # Step 3: Validate structure (không dùng XSD)
        logger.info("Step 3: Validating XML structure...")
        
        validation_errors = []
        validation_warnings = []
        
        # Check root element
        if root.tag != 'GIAMDINHHS':
            validation_errors.append(f"Invalid root element: {root.tag}, expected GIAMDINHHS")
        
        # Check THONGTINDONVI
        thongtindonvi = root.find('THONGTINDONVI')
        if thongtindonvi is not None:
            macskcb = thongtindonvi.findtext('MACSKCB')
            if macskcb:
                logger.info(f"✓ Found MACSKCB: {macskcb}")
            else:
                validation_warnings.append("MACSKCB is empty")
        
        # Check THONGTINHOSO
        thongtinhoso = root.find('THONGTINHOSO')
        if thongtinhoso is None:
            validation_errors.append("Missing THONGTINHOSO element")
        else:
            ngaylap = thongtinhoso.findtext('NGAYLAP')
            soluonghoso = thongtinhoso.findtext('SOLUONGHOSO')
            
            logger.info(f"✓ NGAYLAP: {ngaylap}")
            logger.info(f"✓ SOLUONGHOSO: {soluonghoso}")
            
            # Check DANHSACHHOSO
            danhsachhoso = thongtinhoso.find('DANHSACHHOSO')
            if danhsachhoso is None:
                validation_errors.append("Missing DANHSACHHOSO element")
            else:
                hoso_list = danhsachhoso.findall('HOSO')
                logger.info(f"✓ Found {len(hoso_list)} HOSO elements")
                
                # Validate each HOSO
                for idx, hoso in enumerate(hoso_list, 1):
                    filehoso_list = hoso.findall('FILEHOSO')
                    logger.info(f"✓ HOSO #{idx}: Contains {len(filehoso_list)} FILEHOSO")
                    
                    xml_types = []
                    for filehoso in filehoso_list:
                        loaihoso = filehoso.findtext('LOAIHOSO')
                        noidungfile = filehoso.findtext('NOIDUNGFILE', '')
                        
                        if loaihoso:
                            xml_types.append(loaihoso)
                            logger.info(f"  - {loaihoso}: {len(noidungfile)} characters")
                            
                            # Validate nested XML syntax
                            if noidungfile.strip():
                                try:
                                    # Remove BOM from nested XML
                                    nested_xml = noidungfile.strip()
                                    if nested_xml.startswith('\ufeff'):
                                        nested_xml = nested_xml[1:]
                                    
                                    nested_root = ET.fromstring(nested_xml.encode('utf-8'))
                                    logger.info(f"    ✓ {loaihoso} nested XML valid: <{nested_root.tag}>")
                                    
                                    # Count elements in nested XML
                                    if loaihoso == 'XML1':
                                        ma_lk = nested_root.findtext('MA_LK')
                                        ho_ten = nested_root.findtext('HO_TEN')
                                        ma_the = nested_root.findtext('MA_THE_BHYT')
                                        if not ma_the:
                                            ma_the = nested_root.findtext('MA_THE')
                                        
                                        logger.info(f"    MA_LK: {ma_lk}")
                                        logger.info(f"    HO_TEN: {ho_ten}")
                                        logger.info(f"    MA_THE: {ma_the}")
                                        
                                        if not ma_lk:
                                            validation_errors.append(f"{loaihoso}: Missing MA_LK")
                                        
                                    elif loaihoso == 'XML2':
                                        thuoc_count = len(nested_root.findall('.//CHI_TIET_THUOC'))
                                        logger.info(f"    Chi tiết thuốc: {thuoc_count} records")
                                        
                                    elif loaihoso == 'XML3':
                                        dvkt_count = len(nested_root.findall('.//CHI_TIET_DVKT'))
                                        logger.info(f"    Chi tiết DVKT: {dvkt_count} records")
                                        
                                    elif loaihoso == 'XML4':
                                        cls_count = len(nested_root.findall('.//CHI_TIET_CLS'))
                                        logger.info(f"    Chi tiết CLS: {cls_count} records")
                                        
                                    elif loaihoso == 'XML5':
                                        dbls_count = len(nested_root.findall('.//CHI_TIET_DIEN_BIEN_BENH'))
                                        if dbls_count == 0:
                                            dbls_count = len(nested_root.findall('.//DIEN_BIEN_LAM_SANG'))
                                        logger.info(f"    Diễn biến lâm sàng: {dbls_count} records")
                                    
                                except ET.ParseError as nested_error:
                                    validation_errors.append(f"{loaihoso} nested XML invalid: {str(nested_error)}")
                                    logger.error(f"    ✗ {loaihoso} nested XML parse error: {str(nested_error)}")
                            else:
                                validation_warnings.append(f"{loaihoso} has empty NOIDUNGFILE")
                    
                    logger.info(f"  XML types in HOSO #{idx}: {', '.join(xml_types)}")
        
        # Step 4: Business rules validation
        logger.info("Step 4: Validating business rules...")
        
        # Additional validations can be added here
        
        # Step 5: Determine validation status
        is_valid = len(validation_errors) == 0
        
        if validation_errors:
            logger.error(f"Validation FAILED with {len(validation_errors)} errors:")
            for error in validation_errors:
                logger.error(f"  - {error}")
        else:
            logger.info("✓ All validations passed")
        
        if validation_warnings:
            logger.warning(f"Validation has {len(validation_warnings)} warnings:")
            for warning in validation_warnings:
                logger.warning(f"  - {warning}")
        
        # Step 6: Push validation results to XCom
        logger.info("Step 6: Pushing validation results to XCom...")
        
        validation_result = {
            'is_valid': is_valid,
            'validation_status': 'PASSED' if is_valid else 'FAILED',
            'error_count': len(validation_errors),
            'warning_count': len(validation_warnings),
            'errors': validation_errors,
            'warnings': validation_warnings,
            'validated_at': datetime.utcnow().isoformat()
        }
        
        ti.xcom_push(key='validation_result', value=validation_result)
        ti.xcom_push(key='validated_xml', value=xml_content)
        
        logger.info("=" * 80)
        logger.info(f"TASK 2: VALIDATE - Completed")
        logger.info(f"Status: {'✓ PASSED' if is_valid else '✗ FAILED'}")
        logger.info(f"Errors: {len(validation_errors)}, Warnings: {len(validation_warnings)}")
        logger.info("=" * 80)
        
        # Nếu có quá nhiều lỗi nghiêm trọng, raise exception
        if not is_valid and len(validation_errors) > 10:
            raise AirflowFailException(f"Validation failed with {len(validation_errors)} critical errors")
        
        return validation_result
        
    except Exception as e:
        logger.error(f"TASK 2: VALIDATE - Failed with error: {str(e)}")
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
        
        dto = transformer.transform_complete_xml(xml_content)
        
        logger.info(f"Transformation completed successfully")
        logger.info(f"DTO resource type: {dto['resourceType']}")
        logger.info(f"DTO standard: {dto['standard']}")
        
        # Step 4: Extract key information
        logger.info("Step 4: Extracting key information...")
        
        tonghop = dto.get('tonghop', {})
        
        key_info = {
            'MA_LK': tonghop.get('MA_LK'),
            'MA_BN': tonghop.get('MA_BN'),
            'HO_TEN': tonghop.get('HO_TEN'),
            'NGAY_SINH': tonghop.get('NGAY_SINH'),
            'GIOI_TINH': tonghop.get('GIOI_TINH'),
            'MA_THE': tonghop.get('MA_THE'),
            'MA_CSKCB': tonghop.get('MA_CSKCB'),
            'NGAY_VAO': tonghop.get('NGAY_VAO'),
            'NGAY_RA': tonghop.get('NGAY_RA'),
            'T_TONGCHI': tonghop.get('T_TONGCHI'),
            'T_BHTT': tonghop.get('T_BHTT'),
            'T_BNTT': tonghop.get('T_BNTT')
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
    TASK 4: Verify DTO với external systems (MOCK DATA)
    
    Input: Complete DTO
    Output: Verification results từ external systems
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("=" * 80)
    logger.info("TASK 4: VERIFY - Starting external system verification")
    logger.info("=" * 80)
    
    try:
        # Step 1: Pull DTO from previous task
        logger.info("Step 1: Pulling DTO from previous task...")
        
        dto = ti.xcom_pull(task_ids='transform_to_dto', key='complete_dto')
        dto_summary = ti.xcom_pull(task_ids='transform_to_dto', key='dto_summary')
        
        if not dto:
            raise ValueError("No DTO received from transform task")
        
        tonghop = dto.get('tonghop', {})
        
        # Step 2: Verify Patient Identity (MOCK)
        logger.info("Step 2: Verifying patient identity with National ID System (MOCK)...")
        
        patient_verification = verify_patient_identity_mock(
            ma_bn=tonghop.get('MA_BN'),
            ho_ten=tonghop.get('HO_TEN'),
            ngay_sinh=tonghop.get('NGAY_SINH'),
            so_cccd=tonghop.get('SO_CCCD')
        )
        
        logger.info(f"Patient verification: {patient_verification['status']}")
        logger.info(f"Match score: {patient_verification['match_score']}")
        
        # Step 3: Verify BHYT Card (MOCK)
        logger.info("Step 3: Verifying BHYT card with BHXH Portal (MOCK)...")
        
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
        
        # Step 4: Verify Facility Registration (MOCK)
        logger.info("Step 4: Verifying facility registration (MOCK)...")
        
        facility_verification = verify_facility_mock(
            ma_cskcb=tonghop.get('MA_CSKCB'),
            ma_khoa=tonghop.get('MA_KHOA')
        )
        
        logger.info(f"Facility verification: {facility_verification['status']}")
        logger.info(f"Facility name: {facility_verification['facility_info']['facility_name']}")
        
        # Step 5: Verify Medication Codes (MOCK - Sample only)
        logger.info("Step 5: Verifying medication codes (MOCK - sample)...")
        
        thuoc_list = dto.get('chitiet_thuoc', [])
        medication_verification = verify_medications_mock(thuoc_list[:5])  # Sample first 5
        
        logger.info(f"Verified {len(medication_verification['verified_items'])} medications")
        logger.info(f"Valid: {medication_verification['valid_count']}, "
                   f"Invalid: {medication_verification['invalid_count']}")
        
        # Step 6: Verify Service Codes (MOCK - Sample only)
        logger.info("Step 6: Verifying service codes (MOCK - sample)...")
        
        dvkt_list = dto.get('chitiet_dvkt', [])
        service_verification = verify_services_mock(dvkt_list[:5])  # Sample first 5
        
        logger.info(f"Verified {len(service_verification['verified_items'])} services")
        
        # Step 7: Cross-check Cost Calculation (MOCK)
        logger.info("Step 7: Cross-checking cost calculation...")
        
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
        logger.info(f"Discrepancy: {cost_verification['discrepancy_amount']} VND")
        
        # Step 8: Compile verification results
        logger.info("Step 8: Compiling verification results...")
        
        verification_results = {
            'overall_status': 'VERIFIED',
            'verification_timestamp': datetime.utcnow().isoformat(),
            'ma_lk': tonghop.get('MA_LK'),
            
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
        
        final_json = {
            # Pipeline metadata
            'pipeline_metadata': {
                'pipeline_name': 'bhyt_complete_etl_pipeline_v2',
                'execution_date': context['execution_date'].isoformat(),
                'dag_run_id': context['dag_run'].run_id,
                'task_instance_key': f"{context['dag'].dag_id}.{context['run_id']}",
                'processed_at': datetime.utcnow().isoformat(),
                'pipeline_version': '2.0.0'
            },
            
            # Source information
            'source': {
                's3_uri': file_metadata.get('s3_uri'),
                'message_id': file_metadata.get('message_id'),
                'file_size': file_metadata.get('file_size'),
                'received_at': file_metadata.get('received_at')
            },
            
            # Validation results
            'validation': {
                'status': validation_result.get('validation_status'),
                'is_valid': validation_result.get('is_valid'),
                'error_count': validation_result.get('error_count'),
                'warning_count': validation_result.get('warning_count'),
                'errors': validation_result.get('errors', []),
                'warnings': validation_result.get('warnings', [])
            },
            
            # BHYT Data (Complete DTO)
            'data': dto,
            
            # Verification results
            'verification': verification_results,
            
            # Processing status
            'processing_status': {
                'overall_status': 'SUCCESS',
                'load_status': 'completed',
                'validation_status': validation_result.get('validation_status'),
                'transform_status': 'completed',
                'verification_status': verification_results.get('overall_status'),
                'output_status': 'completed'
            }
        }
        
        # Determine overall processing status
        if not validation_result.get('is_valid'):
            final_json['processing_status']['overall_status'] = 'VALIDATION_FAILED'
        elif verification_results.get('overall_status') == 'FAILED':
            final_json['processing_status']['overall_status'] = 'VERIFICATION_FAILED'
        elif verification_results.get('overall_status') == 'WARNING':
            final_json['processing_status']['overall_status'] = 'SUCCESS_WITH_WARNINGS'
        
        # Step 3: Generate JSON string
        logger.info("Step 3: Generating JSON string...")
        
        json_string = json.dumps(final_json, ensure_ascii=False, indent=2)
        json_size = len(json_string)
        
        logger.info(f"JSON generated successfully. Size: {json_size:,} bytes")
        
        # Step 4: Save JSON to local file (temporary)
        logger.info("Step 4: Saving JSON to local file...")
        
        ma_lk = dto.get('tonghop', {}).get('MA_LK', 'unknown')
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
        
        api_endpoint = Variable.get('bhyt_api_endpoint', 'https://localhost:44315/api/app/files/test')
        
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
        
        # Step 7: Delete message from SQS (processing completed)
        logger.info("Step 7: Deleting message from SQS queue...")
        
        # receipt_handle = file_metadata.get('sqs_receipt_handle')
        # if receipt_handle:
        #     try:
        #         sqs_hook = SqsHook(aws_conn_id='aws_default')
        #         queue_url = Variable.get('bhyt_sqs_queue_url')
                
        #         sqs_hook.delete_message(
        #             queue_url=queue_url,
        #             receipt_handle=receipt_handle
        #         )
        #         logger.info("SQS message deleted successfully")
        #     except Exception as sqs_error:
        #         logger.warning(f"Failed to delete SQS message: {str(sqs_error)}")
        
        # Step 8: Push final results to XCom
        logger.info("Step 8: Pushing final results to XCom...")
        
        final_results = {
            'output_s3_uri': output_s3_uri,
            'output_filename': output_filename,
            'output_local_path': output_path,
            'json_size': json_size,
            'ma_lk': ma_lk,
            'processing_status': final_json['processing_status']['overall_status'],
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
        logger.info(f"Processing Status: {final_json['processing_status']['overall_status']}")
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
                logger.info(f"✓ API call successful (status {response.status_code})")
                
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
