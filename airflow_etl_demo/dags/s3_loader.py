"""
S3 Loader Module - Download và decode XML files từ S3
Hỗ trợ file được mã hóa base64
"""

# import boto3
# from botocore.exceptions import ClientError
import base64
import gzip
import zipfile
import io
import logging
from typing import Optional, Dict, Any


class S3FileLoader:
    """
    Load và decode XML files từ S3 bucket
    Hỗ trợ: base64 encoding, gzip compression, zip files
    """
    
    def __init__(self, aws_access_key: Optional[str] = None, 
                 aws_secret_key: Optional[str] = None,
                 region_name: str = 'ap-southeast-1'):
        self.logger = logging.getLogger(__name__)
        
        # Khởi tạo S3 client
        # if aws_access_key and aws_secret_key:
        #     self.s3_client = boto3.client(
        #         's3',
        #         aws_access_key_id=aws_access_key,
        #         aws_secret_access_key=aws_secret_key,
        #         region_name=region_name
        #     )
        # else:
        #     # Sử dụng credentials từ IAM role hoặc env vars
        #     self.s3_client = boto3.client('s3', region_name=region_name)
    
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
            
        # except ClientError as e:
        #     error_code = e.response['Error']['Code']
        #     if error_code == 'NoSuchKey':
        #         self.logger.error(f"File not found: {s3_key}")
        #     elif error_code == 'NoSuchBucket':
        #         self.logger.error(f"Bucket not found: {bucket_name}")
        #     else:
        #         self.logger.error(f"S3 error: {str(e)}")
        #     raise
        except Exception as e:
            self.logger.error(f"Failed to download/decode file: {str(e)}")
            raise
    
    def _detect_and_decode(self, file_content: bytes, filename: str) -> str:
        """
        Detect file type và decode phù hợp
        Thứ tự kiểm tra:
        1. XML with base64-encoded content in <NOIDUNGFILE>
        2. Base64-encoded XML
        3. Gzip compressed
        4. ZIP file
        5. Plain XML
        """
        # Try to decode as UTF-8 first to check for XML wrapper
        try:
            xml_string = file_content.decode('utf-8')
            if '<NOIDUNGFILE>' in xml_string:
                self.logger.info("Detected XML with base64-encoded contents")
                # Extract and decode all <NOIDUNGFILE> contents while preserving structure
                import re
                
                def decode_base64_content(match):
                    encoded_content = match.group(1).strip()
                    try:
                        decoded_content = self._decode_base64(encoded_content.encode('utf-8'))
                        # Remove XML declaration from nested XML content
                        decoded_content = decoded_content.replace('<?xml version="1.0" encoding="utf-8"?>', '').strip()
                        return f"<NOIDUNGFILE>{decoded_content}</NOIDUNGFILE>"
                    except Exception as e:
                        self.logger.warning(f"Failed to decode content: {str(e)}")
                        return match.group(0)  # Return original if decode fails
                
                # Replace all <NOIDUNGFILE> contents with decoded versions
                decoded_xml = re.sub(
                    r'<NOIDUNGFILE>(.*?)</NOIDUNGFILE>', 
                    decode_base64_content, 
                    xml_string, 
                    flags=re.DOTALL
                )
                return decoded_xml
                
            elif xml_string.strip().startswith('<?xml') or xml_string.strip().startswith('<'):
                self.logger.info("Detected plain XML content")
                return xml_string
        except UnicodeDecodeError:
            pass
        
        # Kiểm tra nếu là base64-encoded
        if self._is_base64_encoded(file_content):
            self.logger.info("Detected base64-encoded content")
            return self._decode_base64(file_content)
        
        
        
        # Nếu không detect được, thử decode base64 (fallback)
        self.logger.warning("Unable to detect format, trying base64 decode")
        return self._decode_base64(file_content)
    
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
                     region_name: str = 'ap-southeast-1') -> S3FileLoader:
    """Factory function để tạo S3FileLoader"""
    return S3FileLoader(aws_access_key, aws_secret_key, region_name)
