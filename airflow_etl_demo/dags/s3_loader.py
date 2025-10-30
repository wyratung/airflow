"""
S3/MinIO Loader Module - Download và decode XML files từ S3 hoặc MinIO
Hỗ trợ file được mã hóa base64
"""

import boto3
from botocore.exceptions import ClientError
from botocore.client import Config
import base64
import gzip
import zipfile
import io
import logging
from typing import Optional, Dict, Any


class S3FileLoader:
    """
    Load và decode XML files từ S3/MinIO bucket
    Hỗ trợ: base64 encoding, gzip compression, zip files
    Hỗ trợ cả AWS S3 và MinIO
    """
    
    def __init__(self, 
                 endpoint_url: Optional[str] = None,
                 aws_access_key: Optional[str] = None, 
                 aws_secret_key: Optional[str] = None,
                 region_name: str = 'us-east-1',
                 use_ssl: bool = True):
        """
        Initialize S3/MinIO client
        
        Args:
            endpoint_url: MinIO endpoint (e.g., 'http://192.168.100.17:9000')
            aws_access_key: Access key (MinIO: AccessKey, AWS: access key)
            aws_secret_key: Secret key (MinIO: SecretKey, AWS: secret key)
            region_name: Region name (for AWS, ignored for MinIO)
            use_ssl: Use SSL/TLS connection
        """
        self.logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.use_ssl = use_ssl
        
        # Khởi tạo S3/MinIO client
        if endpoint_url:
            # MinIO configuration
            self.logger.info(f"Initializing MinIO client with endpoint: {endpoint_url}")
            
            self.s3_client = boto3.client(
                's3',
                endpoint_url=endpoint_url,
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                config=Config(signature_version='s3v4'),
                region_name=region_name,
                use_ssl=use_ssl,
                verify=False  # Disable SSL verification for self-signed certs
            )
        elif aws_access_key and aws_secret_key:
            # AWS S3 with credentials
            self.logger.info("Initializing AWS S3 client with credentials")
            
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=region_name
            )
        else:
            # AWS S3 với IAM role hoặc env vars
            self.logger.info("Initializing AWS S3 client with default credentials")
            self.s3_client = boto3.client('s3', region_name=region_name)
    
    def download_and_decode_file(self, bucket_name: str, s3_key: str) -> str:
        """
        Download file từ S3 và decode về XML string
        
        Args:
            bucket_name: Tên S3 bucket
            s3_key: Key của file trong bucket
            
        Returns:
            XML content dạng string (đã decode)
        """
        self.logger.info(f"Downloading file from s3://{bucket_name}/{s3_key}")
        
        try:
            # Download file từ S3
            response = self.s3_client.get_object(Bucket=bucket_name, Key=s3_key)
            file_content = response['Body'].read()
            
            self.logger.info(f"Downloaded {len(file_content)} bytes")
            
            # Detect và decode file type
            xml_content = self._detect_and_decode(file_content, s3_key)
            
            self.logger.info(f"Successfully decoded to {len(xml_content)} characters")
            return xml_content
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                self.logger.error(f"File not found: {s3_key}")
            elif error_code == 'NoSuchBucket':
                self.logger.error(f"Bucket not found: {bucket_name}")
            else:
                self.logger.error(f"S3 error: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to download/decode file: {str(e)}")
            raise
    
    def _detect_and_decode(self, file_content: bytes, filename: str) -> str:
        """
        Detect file type và decode phù hợp
        Thứ tự kiểm tra:
        1. Plain XML (KHÔNG decode nested XML trong <NOIDUNGFILE>)
        2. Base64-encoded wrapper XML
        3. Gzip compressed
        4. ZIP file
        
        NOTE: Các nested XML trong <NOIDUNGFILE> sẽ KHÔNG được decode
              và giữ nguyên dạng base64-encoded để các bước sau xử lý
        """
        # Try to decode as UTF-8 first to check for XML wrapper
        try:
            xml_string = file_content.decode('utf-8')
            
            # Check if it's XML content (wrapper or standalone)
            if xml_string.strip().startswith('<?xml') or xml_string.strip().startswith('<'):
                self.logger.info("Detected plain XML content")
                
                # If it contains <NOIDUNGFILE>, log info but DO NOT decode
                if '<NOIDUNGFILE>' in xml_string:
                    import re
                    noidungfile_count = len(re.findall(r'<NOIDUNGFILE>', xml_string))
                    self.logger.info(f"Found {noidungfile_count} <NOIDUNGFILE> elements - keeping content encoded")
                
                # Return XML as-is without decoding nested content
                return xml_string
                
        except UnicodeDecodeError:
            self.logger.warning("Failed to decode as UTF-8, trying other methods...")
            pass
        
        # Kiểm tra nếu toàn bộ file là base64-encoded (wrapper XML được encode)
        if self._is_base64_encoded(file_content):
            self.logger.info("Detected base64-encoded wrapper XML")
            decoded_xml = self._decode_base64(file_content)
            
            # After decoding wrapper, check for NOIDUNGFILE but don't decode them
            if '<NOIDUNGFILE>' in decoded_xml:
                import re
                noidungfile_count = len(re.findall(r'<NOIDUNGFILE>', decoded_xml))
                self.logger.info(f"Decoded wrapper XML contains {noidungfile_count} <NOIDUNGFILE> elements - keeping nested content encoded")
            
            return decoded_xml
        
        # Kiểm tra gzip
        if file_content[:2] == b'\x1f\x8b':
            self.logger.info("Detected gzip-compressed content")
            return self._decompress_gzip(file_content)
        
        # Kiểm tra ZIP
        if file_content[:4] == b'PK\x03\x04':
            self.logger.info("Detected ZIP file")
            return self._extract_from_zip(file_content)
        
        # Nếu không detect được, thử decode UTF-8 trực tiếp
        try:
            xml_string = file_content.decode('utf-8')
            self.logger.warning("Unable to detect specific format, returning as UTF-8 string")
            return xml_string
        except UnicodeDecodeError:
            self.logger.error("Unable to decode content as UTF-8")
            raise ValueError("Unable to decode file content - unsupported format or encoding")
    
    def _is_base64_encoded(self, content: bytes) -> bool:
        """
        Kiểm tra xem content có phải base64-encoded không
        """
        try:
            # Base64 string thường chỉ chứa [A-Za-z0-9+/=]
            content_str = content.decode('utf-8').strip()
            
            # Kiểm tra format
            if not content_str:
                return False
            
            # Thử decode
            decoded = base64.b64decode(content_str, validate=True)
            
            # Kiểm tra xem decoded content có phải XML không
            decoded_str = decoded.decode('utf-8', errors='ignore')
            if decoded_str.strip().startswith('<?xml') or decoded_str.strip().startswith('<'):
                return True
            
            # Kiểm tra xem có phải base64 gzip không
            if decoded[:2] == b'\x1f\x8b':
                return True
            
            return False
        except Exception:
            return False
    
    def _decode_base64(self, content: bytes) -> str:
        """
        Decode base64-encoded content
        """
        try:
            # Nếu content đã là bytes, convert sang string
            if isinstance(content, bytes):
                content_str = content.decode('utf-8')
            else:
                content_str = content
            
            # Remove whitespace và newlines
            content_str = content_str.strip().replace('\n', '').replace('\r', '').replace(' ', '')
            
            # Decode base64
            decoded_bytes = base64.b64decode(content_str)
            
            # Kiểm tra xem decoded content có phải gzip không
            if decoded_bytes[:2] == b'\x1f\x8b':
                self.logger.info("Base64 content is gzip-compressed, decompressing...")
                return self._decompress_gzip(decoded_bytes)
            
            # Convert to string
            xml_string = decoded_bytes.decode('utf-8')
            
            return xml_string
            
        except Exception as e:
            self.logger.error(f"Base64 decode failed: {str(e)}")
            raise ValueError(f"Failed to decode base64 content: {str(e)}")
    
    def _decompress_gzip(self, content: bytes) -> str:
        """
        Decompress gzip-compressed content
        """
        try:
            decompressed = gzip.decompress(content)
            xml_string = decompressed.decode('utf-8')
            return xml_string
        except Exception as e:
            self.logger.error(f"Gzip decompression failed: {str(e)}")
            raise ValueError(f"Failed to decompress gzip content: {str(e)}")
    
    def _extract_from_zip(self, content: bytes) -> str:
        """
        Extract XML từ ZIP file
        """
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zip_file:
                # Tìm file XML đầu tiên trong ZIP
                xml_files = [f for f in zip_file.namelist() 
                           if f.endswith('.xml') and not f.startswith('__MACOSX')]
                
                if not xml_files:
                    raise ValueError("No XML file found in ZIP archive")
                
                # Extract file XML đầu tiên
                xml_filename = xml_files[0]
                self.logger.info(f"Extracting {xml_filename} from ZIP")
                
                with zip_file.open(xml_filename) as xml_file:
                    xml_content = xml_file.read()
                    
                    # Kiểm tra xem có phải base64 encoded không
                    if self._is_base64_encoded(xml_content):
                        return self._decode_base64(xml_content)
                    
                    return xml_content.decode('utf-8')
                    
        except zipfile.BadZipFile as e:
            self.logger.error(f"Invalid ZIP file: {str(e)}")
            raise ValueError(f"Invalid ZIP file: {str(e)}")
        except Exception as e:
            self.logger.error(f"ZIP extraction failed: {str(e)}")
            raise ValueError(f"Failed to extract from ZIP: {str(e)}")
    
    def list_files_in_bucket(self, bucket_name: str, prefix: str = '') -> list:
        """
        List tất cả files trong S3 bucket
        
        Args:
            bucket_name: Tên bucket
            prefix: Prefix để filter files
            
        Returns:
            List of file keys
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                return []
            
            files = [obj['Key'] for obj in response['Contents']]
            self.logger.info(f"Found {len(files)} files in s3://{bucket_name}/{prefix}")
            
            return files
            
        except Exception as e:
            self.logger.error(f"Failed to list files: {str(e)}")
            raise
    
    def download_file_to_local(self, bucket_name: str, s3_key: str, 
                               local_path: str) -> str:
        """
        Download file từ S3 về local disk
        
        Args:
            bucket_name: Tên bucket
            s3_key: Key của file
            local_path: Đường dẫn local để save file
            
        Returns:
            Local file path
        """
        try:
            self.s3_client.download_file(bucket_name, s3_key, local_path)
            self.logger.info(f"Downloaded file to {local_path}")
            return local_path
        except Exception as e:
            self.logger.error(f"Failed to download file to local: {str(e)}")
            raise


def create_s3_loader(aws_access_key: Optional[str] = None,
                     aws_secret_key: Optional[str] = None,
                     region_name: str = 'us-east-1',
                     endpoint_url: Optional[str] = None,
                     use_ssl: bool = True) -> S3FileLoader:
    """
    Factory function để tạo S3FileLoader
    
    Args:
        aws_access_key: Access key
        aws_secret_key: Secret key
        region_name: Region name
        endpoint_url: MinIO endpoint URL (e.g., 'http://192.168.100.17:9000')
        use_ssl: Use SSL/TLS
    """
    return S3FileLoader(
        endpoint_url=endpoint_url,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        region_name=region_name,
        use_ssl=use_ssl
    )


def create_minio_loader(endpoint: str, access_key: str, secret_key: str, 
                        use_ssl: bool = False) -> S3FileLoader:
    """
    Factory function để tạo MinIO loader
    
    Args:
        endpoint: MinIO endpoint (e.g., '192.168.100.17:9000')
        access_key: MinIO access key
        secret_key: MinIO secret key
        use_ssl: Use SSL/TLS
    
    Returns:
        S3FileLoader configured for MinIO
    """
    # Add http:// or https:// prefix if not present
    if not endpoint.startswith('http://') and not endpoint.startswith('https://'):
        protocol = 'https://' if use_ssl else 'http://'
        endpoint_url = f"{protocol}{endpoint}"
    else:
        endpoint_url = endpoint
    
    return S3FileLoader(
        endpoint_url=endpoint_url,
        aws_access_key=access_key,
        aws_secret_key=secret_key,
        region_name='us-east-1',  # MinIO doesn't care about region
        use_ssl=use_ssl
    )
