"""
Complete BHYT Data Transformer - Theo đúng QĐ 4750/QĐ-BYT
Parse đầy đủ 15 bảng XML với tên CHUẨN từ XSD
Phiên bản: 3.0 - Updated với tên bảng chính xác
"""

import xml.etree.ElementTree as ET
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
import re


class CompleteBHYTTransformer:
    """
    BHYT Transformer - Plain Text XML Only
    Hỗ trợ 15 bảng theo QĐ 4750
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.encoding = 'UTF-8'
    
    def transform_complete_xml(self, xml_content: str) -> Dict[str, Any]:
        """
        Parse đầy đủ 15 bảng XML BHYT
        Input: XML wrapper GIAMDINHHS
        Output: DTO với 15 bảng
        """
        self.logger.info("=" * 80)
        self.logger.info("BHYT Transformer v3.2 - Plain Text XML Only")
        self.logger.info("=" * 80)
        
        try:
            # Remove BOM
            if xml_content.startswith('\ufeff'):
                xml_content = xml_content[1:]
                self.logger.info("Removed BOM")
            
            # Parse wrapper XML
            root = ET.fromstring(xml_content.encode(self.encoding))
            self.logger.info(f"✓ Root element: {root.tag}")
            
        except Exception as e:
            self.logger.error(f"XML parsing failed: {str(e)}")
            raise ValueError(f"Invalid XML content: {str(e)}")
        
        # Parse header
        header = self._parse_header(root)
        self.logger.info(f"✓ Header: MACSKCB={header.get('MACSKCB')}, NGAYLAP={header.get('NGAYLAP')}")
        
        # Parse all HOSO
        records = []
        hoso_list = root.findall('.//HOSO')
        self.logger.info(f"✓ Found {len(hoso_list)} HOSO elements")
        
        for hoso_idx, hoso in enumerate(hoso_list, 1):
            self.logger.info(f"\nProcessing HOSO #{hoso_idx}...")
            record = self._parse_single_hoso(hoso, hoso_idx)
            
            if record and record.get('TONG_HOP'):
                records.append(record)
                ma_lk = record['TONG_HOP'].get('MA_LK', 'N/A')
                self.logger.info(f"✓ HOSO #{hoso_idx} added (MA_LK: {ma_lk})")
            else:
                self.logger.warning(f"✗ HOSO #{hoso_idx} skipped - missing TONG_HOP")
        
        # Build final DTO
        dto = {
            'resourceType': 'BHYTClaimDocument_QD4750',
            'standard': 'QD_4750_BYT_v2.0',
            'version': '3.2',
            'header': header,
            'records': records,
            'metadata': {
                'transformedAt': datetime.utcnow().isoformat(),
                'transformer': 'CompleteBHYTTransformer_v3.2_PlainText',
                'totalRecords': len(records),
                'xmlTypes': self._count_xml_types(records)
            }
        }
        
        self.logger.info("\n" + "=" * 80)
        self.logger.info(f"✓ Transformation completed!")
        self.logger.info(f"Total records: {len(records)}")
        self.logger.info(f"XML types: {list(dto['metadata']['xmlTypes'].keys())}")
        self.logger.info("=" * 80)
        
        return dto
    
    def _parse_single_hoso(self, hoso_elem: ET.Element, hoso_idx: int) -> Dict[str, Any]:
        """Parse 1 HOSO chứa tất cả FILEHOSO"""
        record = {}
        
        filehoso_list = hoso_elem.findall('FILEHOSO')
        self.logger.info(f"  Found {len(filehoso_list)} FILEHOSO")
        
        for filehoso in filehoso_list:
            loaihoso = filehoso.findtext('LOAIHOSO', '').strip()
            
            if not loaihoso:
                continue
            
            # Get NOIDUNGFILE - can be text or nested XML element
            noidungfile_elem = filehoso.find('NOIDUNGFILE')
            if noidungfile_elem is None:
                self.logger.warning(f"  ✗ {loaihoso}: Missing NOIDUNGFILE element")
                continue
            
            # Check if NOIDUNGFILE has child elements (nested XML)
            if len(noidungfile_elem) > 0:
                # Has child elements - serialize inner XML
                inner_xml = ''.join(ET.tostring(child, encoding='unicode') for child in noidungfile_elem)
                noidungfile = inner_xml.strip()
                self.logger.info(f"  ✓ {loaihoso}: Nested XML detected ({len(noidungfile)} chars)")
            else:
                # Plain text content (might be base64 or plain XML string)
                noidungfile = noidungfile_elem.text or ''
                noidungfile = noidungfile.strip()
                if not noidungfile:
                    self.logger.warning(f"  ✗ {loaihoso}: Empty NOIDUNGFILE")
                    continue
                self.logger.info(f"  Processing {loaihoso}: {len(noidungfile)} chars (text content)")
            
            # Clean plain text XML (remove XML declaration if present)
            cleaned_xml = self._clean_nested_xml(noidungfile)
            
            # Parse nested XML
            try:
                nested_root = ET.fromstring(cleaned_xml.encode(self.encoding))
                
                # Route to parser
                if loaihoso == 'XML1':
                    record['TONG_HOP'] = self._parse_xml1_tonghop(nested_root)
                elif loaihoso == 'XML2':
                    record['DSACH_CHI_TIET_THUOC'] = self._parse_xml2_thuoc(nested_root)
                elif loaihoso == 'XML3':
                    record['DSACH_CHI_TIET_DVKT'] = self._parse_xml3_dvkt(nested_root)
                elif loaihoso == 'XML4':
                    record['DSACH_CHI_TIET_CLS'] = self._parse_xml4_cls(nested_root)
                elif loaihoso == 'XML5':
                    record['DSACH_CHI_TIET_DIEN_BIEN_BENH'] = self._parse_xml5_dienbienlamsang(nested_root)
                elif loaihoso == 'XML6':
                    record['DSACH_HO_SO_HIV_AIDS'] = self._parse_xml6_hivaids(nested_root)
                elif loaihoso == 'XML7':
                    record['GIAY_RA_VIEN'] = self._parse_xml7_giayravien(nested_root)
                elif loaihoso == 'XML8':
                    record['TOM_TAT_HO_SO_BA'] = self._parse_xml8_tomtat(nested_root)
                elif loaihoso == 'XML9':
                    record['GIAY_CHUNG_SINH'] = self._parse_xml9_chungsinh(nested_root)
                elif loaihoso == 'XML10':
                    record['GIAY_NGHI_DUONG_THAI'] = self._parse_xml10_nghithai(nested_root)
                elif loaihoso == 'XML11':
                    record['GIAY_NGHI_VIEC_HUONG_BHXH'] = self._parse_xml11_nghibhxh(nested_root)
                elif loaihoso == 'XML12':
                    record['DSACH_GIAM_DINH_Y_KHOA'] = self._parse_xml12_giamdinh(nested_root)
                elif loaihoso == 'XML13':
                    record['DSACH_GIAY_CHUYEN_TUYEN'] = self._parse_xml13_chuyentuyen(nested_root)
                elif loaihoso == 'XML14':
                    record['DSACH_GIAY_HEN_KHAM_LAI'] = self._parse_xml14_henkham(nested_root)
                elif loaihoso == 'XML15':
                    record['DSACH_THONG_TIN_LAO'] = self._parse_xml15_lao(nested_root)
                
                self.logger.info(f"  ✓ {loaihoso} parsed successfully")
                
            except ET.ParseError as e:
                self.logger.error(f"  ✗ {loaihoso} XML parse error: {str(e)}")
            except Exception as e:
                self.logger.error(f"  ✗ {loaihoso} error: {str(e)}")
        
        return record
    
    def _clean_nested_xml(self, xml_content: str) -> str:
        """Clean plain text XML - remove XML declaration and BOM"""
        # Remove BOM
        if xml_content.startswith('\ufeff'):
            xml_content = xml_content[1:]
        
        # Remove XML declaration <?xml version="1.0"?>
        xml_content = re.sub(r'<\?xml[^?]*\?>\s*', '', xml_content)
        
        return xml_content.strip()
    
    def _parse_header(self, root: ET.Element) -> Dict[str, Any]:
        """Parse header từ GIAMDINHHS"""
        return {
            'MACSKCB': self._get_text(root, './/MACSKCB'),
            'NGAYLAP': self._get_text(root, './/NGAYLAP'),
            'SOLUONGHOSO': self._get_int(root, './/SOLUONGHOSO')
        }
    
    # ========================================
    # XML1 - TONG_HOP (OBJECT)
    # ========================================
    
    def _parse_xml1_tonghop(self, root: ET.Element) -> Dict[str, Any]:
        return {
            # Primary Key
            'MA_LK': self._get_text(root, 'MA_LK'),
            'STT': self._get_int(root, 'STT'),
            
            # Thông tin bệnh nhân
            'MA_BN': self._get_text(root, 'MA_BN'),
            'HO_TEN': self._get_text(root, 'HO_TEN'),
            'SO_CCCD': self._get_text(root, 'SO_CCCD'),
            'NGAY_SINH': self._get_text(root, 'NGAY_SINH'),
            'GIOI_TINH': self._get_int(root, 'GIOI_TINH'),
            'NHOM_MAU': self._get_text(root, 'NHOM_MAU'),
            'MA_QUOCTICH': self._get_text(root, 'MA_QUOCTICH'),
            'MA_DANTOC': self._get_text(root, 'MA_DANTOC'),
            'MA_NGHE_NGHIEP': self._get_text(root, 'MA_NGHE_NGHIEP'),
            'DIA_CHI': self._get_text(root, 'DIA_CHI'),
            'MATINH_CU_TRU': self._get_text(root, 'MATINH_CU_TRU'),
            'MAHUYEN_CU_TRU': self._get_text(root, 'MAHUYEN_CU_TRU'),
            'MAXA_CU_TRU': self._get_text(root, 'MAXA_CU_TRU'),
            'DIEN_THOAI': self._get_text(root, 'DIEN_THOAI'),
            
            # Thẻ BHYT
            'MA_THE_BHYT': self._get_text(root, 'MA_THE_BHYT'),
            'MA_DKBD': self._get_text(root, 'MA_DKBD'),
            'GT_THE_TU': self._get_text(root, 'GT_THE_TU'),
            'GT_THE_DEN': self._get_text(root, 'GT_THE_DEN'),
            'NGAY_MIEN_CCT': self._get_text(root, 'NGAY_MIEN_CCT'),
            'LY_DO_MIEN_CCT': self._get_text(root, 'LY_DO_MIEN_CCT'),
            'NGAY_DU_5NAM': self._get_text(root, 'NGAY_DU_5NAM'),
            'MA_KV': self._get_text(root, 'MA_KV'),
            'MA_KHUVUC': self._get_text(root, 'MA_KHUVUC'),
            
            # Thông tin KCB
            'MA_KHOA': self._get_text(root, 'MA_KHOA'),
            'MA_CSKCB': self._get_text(root, 'MA_CSKCB'),
            'MA_LOAI_KCB': self._get_int(root, 'MA_LOAI_KCB'),
            'MA_NOI_DI': self._get_text(root, 'MA_NOI_DI'),
            'MA_NOI_DEN': self._get_text(root, 'MA_NOI_DEN'),
            'MA_TAI_NAN': self._get_text(root, 'MA_TAI_NAN'),
            'MA_DOITUONG_KCB': self._get_text(root, 'MA_DOITUONG_KCB'),
            
            # Chẩn đoán
            'MA_BENH_CHINH': self._get_text(root, 'MA_BENH_CHINH'),
            'MA_BENH_KEM': self._get_text(root, 'MA_BENH_KEM'),
            'MA_BENH_YHCT': self._get_text(root, 'MA_BENH_YHCT'),
            'TEN_BENH': self._get_text(root, 'TEN_BENH'),
            'TOM_TAT_KQ': self._get_text(root, 'TOM_TAT_KQ'),
            'PP_DIEUTRI': self._get_text(root, 'PP_DIEUTRI'),
            'GHI_CHU_DT': self._get_text(root, 'GHI_CHU_DT'),
            'MA_LOAI_RV': self._get_text(root, 'MA_LOAI_RV'),
            
            # Ngày giờ
            'NGAY_VAO': self._get_text(root, 'NGAY_VAO'),
            'NGAY_VAO_NOI_TRU': self._get_text(root, 'NGAY_VAO_NOI_TRU'),
            'NGAY_RA': self._get_text(root, 'NGAY_RA'),
            'NGAY_TTOAN': self._get_text(root, 'NGAY_TTOAN'),
            'NGAY_CT': self._get_text(root, 'NGAY_CT'),
            
            # Kết quả điều trị
            'SO_NGAY_DTRI': self._get_int(root, 'SO_NGAY_DTRI'),
            'KET_QUA_DTRI': self._get_int(root, 'KET_QUA_DTRI'),
            'TINH_TRANG_RV': self._get_int(root, 'TINH_TRANG_RV'),
            'HUONG_DIEU_TRI': self._get_text(root, 'HUONG_DIEU_TRI'),
            
            # Chi phí - FULL 20 fields
            'T_TONGCHI': self._get_float(root, 'T_TONGCHI'),
            'T_TONGCHI_BV': self._get_float(root, 'T_TONGCHI_BV'),
            'T_TONGCHI_BH': self._get_float(root, 'T_TONGCHI_BH'),
            'T_NGOAIDM': self._get_float(root, 'T_NGOAIDM'),
            'T_BHTT': self._get_float(root, 'T_BHTT'),
            'T_BNCCT': self._get_float(root, 'T_BNCCT'),
            'T_BNTT': self._get_float(root, 'T_BNTT'),
            'T_NGUONKHAC': self._get_float(root, 'T_NGUONKHAC'),
            'T_KHAM': self._get_float(root, 'T_KHAM'),
            'T_CDHA': self._get_float(root, 'T_CDHA'),
            'T_XETNGHIEM': self._get_float(root, 'T_XETNGHIEM'),
            'T_THUOC': self._get_float(root, 'T_THUOC'),
            'T_MAU': self._get_float(root, 'T_MAU'),
            'T_PTTT': self._get_float(root, 'T_PTTT'),
            'T_DVKT_TYLE': self._get_float(root, 'T_DVKT_TYLE'),
            'T_THUOC_TYLE': self._get_float(root, 'T_THUOC_TYLE'),
            'T_KHAM_TYLE': self._get_float(root, 'T_KHAM_TYLE'),
            'T_VTYT_TYLE': self._get_float(root, 'T_VTYT_TYLE'),
            'T_GIUONG': self._get_float(root, 'T_GIUONG'),
            'T_VCHUYEN': self._get_float(root, 'T_VCHUYEN'),
            'T_BNTT_VTYT': self._get_float(root, 'T_BNTT_VTYT'),
            
            # Thông tin khác
            'NAM_QT': self._get_int(root, 'NAM_QT'),
            'THANG_QT': self._get_int(root, 'THANG_QT'),
            'MA_LOAIKCB': self._get_int(root, 'MA_LOAIKCB'),
            'MA_HSBA': self._get_text(root, 'MA_HSBA'),
            'CAN_NANG': self._get_float(root, 'CAN_NANG'),
            'DU_PHONG': self._get_text(root, 'DU_PHONG')
        }

    
    # ========================================
    # XML2 - DSACH_CHI_TIET_THUOC (ARRAY)
    # ========================================
    
    def _parse_xml2_thuoc(self, root: ET.Element) -> List[Dict[str, Any]]:
        thuoc_list = []
        
        for elem in root.findall('.//CHI_TIET_THUOC'):
            thuoc = {
                'MA_LK': self._get_text(elem, 'MA_LK'),
                'STT': self._get_int(elem, 'STT'),
                'MA_THUOC': self._get_text(elem, 'MA_THUOC'),
                'MA_PP_CHEBIEN': self._get_text(elem, 'MA_PP_CHEBIEN'),
                'MA_CSKCB_THUOC': self._get_text(elem, 'MA_CSKCB_THUOC'),
                'MA_NHOM': self._get_int(elem, 'MA_NHOM'),
                'TEN_THUOC': self._get_text(elem, 'TEN_THUOC'),
                'DON_VI_TINH': self._get_text(elem, 'DON_VI_TINH'),
                'HAM_LUONG': self._get_text(elem, 'HAM_LUONG'),
                'DUONG_DUNG': self._get_text(elem, 'DUONG_DUNG'),
                'DANG_BAO_CHE': self._get_text(elem, 'DANG_BAO_CHE'),
                'LIEU_DUNG': self._get_text(elem, 'LIEU_DUNG'),
                'CACH_DUNG': self._get_text(elem, 'CACH_DUNG'),
                'SO_DANG_KY': self._get_text(elem, 'SO_DANG_KY'),
                'TT_THAU': self._get_text(elem, 'TT_THAU'),
                'PHAM_VI': self._get_int(elem, 'PHAM_VI'),
                'TYLE_TT_DV': self._get_float(elem, 'TYLE_TT_DV'),
                'SO_LUONG': self._get_float(elem, 'SO_LUONG'),
                'DON_GIA': self._get_float(elem, 'DON_GIA'),
                'TYLE_TT_BH': self._get_float(elem, 'TYLE_TT_BH'),
                'THANH_TIEN': self._get_float(elem, 'THANH_TIEN'),
                'MUC_HUONG': self._get_int(elem, 'MUC_HUONG'),
                'T_TRANTT': self._get_float(elem, 'T_TRANTT'),
                'T_BHTT': self._get_float(elem, 'T_BHTT'),
                'T_BNCCT': self._get_float(elem, 'T_BNCCT'),
                'T_BNTT': self._get_float(elem, 'T_BNTT'),
                'T_NGUONKHAC': self._get_float(elem, 'T_NGUONKHAC'),
                'MA_KHOA': self._get_text(elem, 'MA_KHOA'),
                'MA_BAC_SI': self._get_text(elem, 'MA_BAC_SI'),
                'MA_DICH_VU': self._get_text(elem, 'MA_DICH_VU'),
                'MA_BENH': self._get_text(elem, 'MA_BENH'),
                'NGAY_YL': self._get_text(elem, 'NGAY_YL'),
                'NGAY_TH_YL': self._get_text(elem, 'NGAY_TH_YL'),
                'MA_PTTT': self._get_text(elem, 'MA_PTTT'),
                'NGUON_KHAC': self._get_float(elem, 'NGUON_KHAC'),
                'SO_NGAY': self._get_int(elem, 'SO_NGAY'),
                'SO_LUONG_KEDON': self._get_float(elem, 'SO_LUONG_KEDON'),
                'DU_PHONG': self._get_text(elem, 'DU_PHONG')
            }
            thuoc_list.append(thuoc)
        
        return thuoc_list
    
    # ========================================
    # XML3 - DSACH_CHI_TIET_DVKT (ARRAY)
    # ========================================
    
    def _parse_xml3_dvkt(self, root: ET.Element) -> List[Dict[str, Any]]:
        """XML3: DSACH_CHI_TIET_DVKT (ARRAY)"""
        dvkt_list = []
    
        for elem in root.findall('.//CHI_TIET_DVKT'):
            dvkt = {
                'MA_LK': self._get_text(elem, 'MA_LK'),
                'STT': self._get_int(elem, 'STT'),
                'MA_DICH_VU': self._get_text(elem, 'MA_DICH_VU'),
                'MA_PTTT_QT': self._get_text(elem, 'MA_PTTT_QT'),
                'MA_VAT_TU': self._get_text(elem, 'MA_VAT_TU'),
                'MA_NHOM': self._get_int(elem, 'MA_NHOM'),
                'GOI_VTYT': self._get_text(elem, 'GOI_VTYT'),
                'TEN_VAT_TU': self._get_text(elem, 'TEN_VAT_TU'),
                'TEN_DICH_VU': self._get_text(elem, 'TEN_DICH_VU'),
                'MA_XANG_DAU': self._get_text(elem, 'MA_XANG_DAU'),
                'DON_VI_TINH': self._get_text(elem, 'DON_VI_TINH'),
                'PHAM_VI': self._get_int(elem, 'PHAM_VI'),
                'SO_LUONG': self._get_float(elem, 'SO_LUONG'),
                'DON_GIA_BV': self._get_float(elem, 'DON_GIA_BV'),
                'DON_GIA_BH': self._get_float(elem, 'DON_GIA_BH'),
                'DON_GIA': self._get_float(elem, 'DON_GIA'),
                'TYLE_TT_DV': self._get_float(elem, 'TYLE_TT_DV'),
                'TYLE_TT_BH': self._get_float(elem, 'TYLE_TT_BH'),
                'TT_THAU': self._get_text(elem, 'TT_THAU'),
                'THANH_TIEN_BV': self._get_float(elem, 'THANH_TIEN_BV'),
                'THANH_TIEN_BH': self._get_float(elem, 'THANH_TIEN_BH'),
                'THANH_TIEN': self._get_float(elem, 'THANH_TIEN'),
                'T_TRANTT': self._get_float(elem, 'T_TRANTT'),
                'MUC_HUONG': self._get_int(elem, 'MUC_HUONG'),
                'T_BHTT': self._get_float(elem, 'T_BHTT'),
                'T_BNCCT': self._get_float(elem, 'T_BNCCT'),
                'T_BNTT': self._get_float(elem, 'T_BNTT'),
                'T_NGUONKHAC': self._get_float(elem, 'T_NGUONKHAC'),
                'MA_KHOA': self._get_text(elem, 'MA_KHOA'),
                'MA_GIUONG': self._get_text(elem, 'MA_GIUONG'),
                'MA_BAC_SI': self._get_text(elem, 'MA_BAC_SI'),
                'MA_BENH': self._get_text(elem, 'MA_BENH'),
                'NGAY_YL': self._get_text(elem, 'NGAY_YL'),
                'NGAY_TH_YL': self._get_text(elem, 'NGAY_TH_YL'),
                'NGAY_KQ': self._get_text(elem, 'NGAY_KQ'),
                'MA_PTTT': self._get_text(elem, 'MA_PTTT'),
                'MA_MAYDO': self._get_text(elem, 'MA_MAYDO'),
                'TEN_MAYDO': self._get_text(elem, 'TEN_MAYDO'),
                'MA_HIEU': self._get_text(elem, 'MA_HIEU'),
                'VET_THUONG_TP': self._get_text(elem, 'VET_THUONG_TP'),
                'PP_VO_CAM': self._get_text(elem, 'PP_VO_CAM'),
                'VI_TRI_TH_DVKT': self._get_text(elem, 'VI_TRI_TH_DVKT'),
                'MA_MAY_XN': self._get_text(elem, 'MA_MAY_XN'),
                'KET_QUA': self._get_text(elem, 'KET_QUA'),
                'NGUOI_THUC_HIEN': self._get_text(elem, 'NGUOI_THUC_HIEN'),
                'DU_PHONG': self._get_text(elem, 'DU_PHONG')
            }
            dvkt_list.append(dvkt)
        
        return dvkt_list
    
    # ========================================
    # XML4 - DSACH_CHI_TIET_CLS (ARRAY)
    # ========================================
    
    def _parse_xml4_cls(self, root: ET.Element) -> List[Dict[str, Any]]:
        """XML4: DSACH_CHI_TIET_CLS - Chi tiết cận lâm sàng (ARRAY)"""
        cls_list = []
    
        for elem in root.findall('.//CHI_TIET_CLS'):
            cls = {
                'MA_LK': self._get_text(elem, 'MA_LK'),
                'STT': self._get_int(elem, 'STT'),
                'MA_DICH_VU': self._get_text(elem, 'MA_DICH_VU'),
                'MA_CHI_SO': self._get_text(elem, 'MA_CHI_SO'),
                'TEN_CHI_SO': self._get_text(elem, 'TEN_CHI_SO'),
                'GIA_TRI': self._get_text(elem, 'GIA_TRI'),
                'DON_VI_DO': self._get_text(elem, 'DON_VI_DO'),
                'MO_TA': self._get_text(elem, 'MO_TA'),
                'KET_LUAN': self._get_text(elem, 'KET_LUAN'),
                'NGAY_KQ': self._get_text(elem, 'NGAY_KQ'),
                'MA_BS_DOC_KQ': self._get_text(elem, 'MA_BS_DOC_KQ')
            }
            cls_list.append(cls)
        
        return cls_list
    
    # ========================================
    # XML5 - DSACH_CHI_TIET_DIEN_BIEN_BENH (ARRAY)
    # ========================================
    
    def _parse_xml5_dienbienlamsang(self, root: ET.Element) -> List[Dict[str, Any]]:
        """XML5: DSACH_CHI_TIET_DIEN_BIEN_BENH - Diễn biến lâm sàng (ARRAY)"""
        dbls_list = []
    
        for elem in root.findall('.//CHI_TIET_DIEN_BIEN_BENH'):
            dbls = {
                'MA_LK': self._get_text(elem, 'MA_LK'),
                'STT': self._get_int(elem, 'STT'),
                'DIEN_BIEN_LS': self._get_text(elem, 'DIEN_BIEN_LS'),
                'GIAI_DOAN_BENH': self._get_text(elem, 'GIAI_DOAN_BENH'),
                'HOI_CHAN': self._get_text(elem, 'HOI_CHAN'),
                'PHAU_THUAT': self._get_text(elem, 'PHAU_THUAT'),
                'THOI_DIEM_DBLS': self._get_text(elem, 'THOI_DIEM_DBLS'),
                'NGUOI_THUC_HIEN': self._get_text(elem, 'NGUOI_THUC_HIEN'),
                'DU_PHONG': self._get_text(elem, 'DU_PHONG')
            }
            dbls_list.append(dbls)
        
        return dbls_list
    
    # ========================================
    # XML6 - DSACH_HO_SO_HIV_AIDS (ARRAY)
    # ========================================
    
    def _parse_xml6_hivaids(self, root: ET.Element) -> List[Dict[str, Any]]:
        """XML6: DSACH_HO_SO_HIV_AIDS - Hồ sơ HIV/AIDS (ARRAY)"""
        hiv_list = []
    
        for elem in root.findall('.//HO_SO_HIV_AIDS'):
            hiv = {
                'MA_LK': self._get_text(elem, 'MA_LK'),
                'STT': self._get_int(elem, 'STT'),
                'MA_THE_BHYT': self._get_text(elem, 'MA_THE_BHYT'),
                'SO_CCCD': self._get_text(elem, 'SO_CCCD'),
                'NGAY_SINH': self._get_text(elem, 'NGAY_SINH'),
                'GIOI_TINH': self._get_int(elem, 'GIOI_TINH'),
                'DIA_CHI': self._get_text(elem, 'DIA_CHI'),
                'MATINH_CU_TRU': self._get_text(elem, 'MATINH_CU_TRU'),
                'MAHUYEN_CU_TRU': self._get_text(elem, 'MAHUYEN_CU_TRU'),
                'MAXA_CU_TRU': self._get_text(elem, 'MAXA_CU_TRU'),
                'NGAYKD_HIV': self._get_text(elem, 'NGAYKD_HIV'),
                'NOI_LAY_MAU_XN': self._get_text(elem, 'NOI_LAY_MAU_XN'),
                'NOI_XN_KD': self._get_text(elem, 'NOI_XN_KD'),
                'NOI_BDDT_ARV': self._get_text(elem, 'NOI_BDDT_ARV'),
                'BDDT_ARV': self._get_text(elem, 'BDDT_ARV'),
                'MA_PHAC_DO_DIEU_TRI_BD': self._get_text(elem, 'MA_PHAC_DO_DIEU_TRI_BD'),
                'MA_BAC_PHAC_DO_BD': self._get_text(elem, 'MA_BAC_PHAC_DO_BD'),
                'MA_LYDO_DTRI': self._get_text(elem, 'MA_LYDO_DTRI'),
                'LOAI_DTRI_LAO': self._get_text(elem, 'LOAI_DTRI_LAO'),
                'SANG_LOC_LAO': self._get_text(elem, 'SANG_LOC_LAO'),
                'PHACDO_DTRI_LAO': self._get_text(elem, 'PHACDO_DTRI_LAO'),
                'NGAYBD_DTRI_LAO': self._get_text(elem, 'NGAYBD_DTRI_LAO'),
                'NGAYKT_DTRI_LAO': self._get_text(elem, 'NGAYKT_DTRI_LAO'),
                'KQ_DTRI_LAO': self._get_text(elem, 'KQ_DTRI_LAO'),
                'MA_LYDO_XNTL_VR': self._get_text(elem, 'MA_LYDO_XNTL_VR'),
                'NGAY_XN_TLVR': self._get_text(elem, 'NGAY_XN_TLVR'),
                'KQ_XNTL_VR': self._get_text(elem, 'KQ_XNTL_VR'),
                'NGAY_KQ_XN_TLVR': self._get_text(elem, 'NGAY_KQ_XN_TLVR'),
                'MA_LOAI_BN': self._get_text(elem, 'MA_LOAI_BN'),
                'GIAI_DOAN_LAM_SANG': self._get_text(elem, 'GIAI_DOAN_LAM_SANG'),
                'NHOM_DOI_TUONG': self._get_text(elem, 'NHOM_DOI_TUONG'),
                'MA_TINH_TRANG_DK': self._get_text(elem, 'MA_TINH_TRANG_DK'),
                'LAN_XN_PCR': self._get_int(elem, 'LAN_XN_PCR'),
                'NGAY_XN_PCR': self._get_text(elem, 'NGAY_XN_PCR'),
                'NGAY_KQ_XN_PCR': self._get_text(elem, 'NGAY_KQ_XN_PCR'),
                'MA_KQ_XN_PCR': self._get_text(elem, 'MA_KQ_XN_PCR'),
                'NGAY_NHAN_TT_MANG_THAI': self._get_text(elem, 'NGAY_NHAN_TT_MANG_THAI'),
                'NGAY_BAT_DAU_DT_CTX': self._get_text(elem, 'NGAY_BAT_DAU_DT_CTX'),
                'MA_XU_TRI': self._get_text(elem, 'MA_XU_TRI'),
                'NGAY_BAT_DAU_XU_TRI': self._get_text(elem, 'NGAY_BAT_DAU_XU_TRI'),
                'NGAY_KET_THUC_XU_TRI': self._get_text(elem, 'NGAY_KET_THUC_XU_TRI'),
                'MA_PHAC_DO_DIEU_TRI': self._get_text(elem, 'MA_PHAC_DO_DIEU_TRI'),
                'MA_BAC_PHAC_DO': self._get_text(elem, 'MA_BAC_PHAC_DO'),
                'SO_NGAY_CAP_THUOC_ARV': self._get_int(elem, 'SO_NGAY_CAP_THUOC_ARV'),
                'NGAY_CHUYEN_PHAC_DO': self._get_text(elem, 'NGAY_CHUYEN_PHAC_DO'),
                'LY_DO_CHUYEN_PHAC_DO': self._get_text(elem, 'LY_DO_CHUYEN_PHAC_DO'),
                'MA_CSKCB': self._get_text(elem, 'MA_CSKCB'),
                'DU_PHONG': self._get_text(elem, 'DU_PHONG')
            }
            hiv_list.append(hiv)
        
        return hiv_list
    
    # ========================================
    # XML7 - GIAY_RA_VIEN (OBJECT)
    # ========================================
    
    def _parse_xml7_giayravien(self, root: ET.Element) -> Optional[Dict[str, Any]]:
        """XML7: GIAY_RA_VIEN - Giấy ra viện (SINGLE OBJECT)"""
        if root is None:
            return None
    
        return {
            'MA_LK': self._get_text(root, 'MA_LK'),
            'SO_LUU_TRU': self._get_text(root, 'SO_LUU_TRU'),
            'MA_YTE': self._get_text(root, 'MA_YTE'),
            'MA_KHOA_RV': self._get_text(root, 'MA_KHOA_RV'),
            'NGAY_VAO': self._get_text(root, 'NGAY_VAO'),
            'NGAY_RA': self._get_text(root, 'NGAY_RA'),
            'MA_DINH_CHI_THAI': self._get_text(root, 'MA_DINH_CHI_THAI'),
            'NGUYENNHAN_DINHCHI': self._get_text(root, 'NGUYENNHAN_DINHCHI'),
            'THOIGIAN_DINHCHI': self._get_text(root, 'THOIGIAN_DINHCHI'),
            'TUOI_THAI': self._get_int(root, 'TUOI_THAI'),
            'CHAN_DOAN_RV': self._get_text(root, 'CHAN_DOAN_RV'),
            'PP_DIEUTRI': self._get_text(root, 'PP_DIEUTRI'),
            'GHI_CHU': self._get_text(root, 'GHI_CHU'),
            'MA_TTDV': self._get_text(root, 'MA_TTDV'),
            'MA_BS': self._get_text(root, 'MA_BS'),
            'TEN_BS': self._get_text(root, 'TEN_BS'),
            'NGAY_CT': self._get_text(root, 'NGAY_CT'),
            'MA_CHA': self._get_text(root, 'MA_CHA'),
            'MA_ME': self._get_text(root, 'MA_ME'),
            'MA_THE_TAM': self._get_text(root, 'MA_THE_TAM'),
            'HO_TEN_CHA': self._get_text(root, 'HO_TEN_CHA'),
            'HO_TEN_ME': self._get_text(root, 'HO_TEN_ME'),
            'SO_NGAY_NGHI': self._get_int(root, 'SO_NGAY_NGHI'),
            'NGOAITRU_TUNGAY': self._get_text(root, 'NGOAITRU_TUNGAY'),
            'NGOAITRU_DENNGAY': self._get_text(root, 'NGOAITRU_DENNGAY'),
            'DU_PHONG': self._get_text(root, 'DU_PHONG')
        }
    
    # ========================================
    # XML8 - TOM_TAT_HO_SO_BA (OBJECT)
    # ========================================
    
    def _parse_xml8_tomtat(self, root: ET.Element) -> Optional[Dict[str, Any]]:
        """XML8: TOM_TAT_HO_SO_BA - Tóm tắt hồ sơ bệnh án (SINGLE OBJECT)"""
        if root is None:
            return None
        
        return {
            'MA_LK': self._get_text(root, 'MA_LK'),
            'MA_LOAI_KCB': self._get_int(root, 'MA_LOAI_KCB'),
            'HO_TEN_CHA': self._get_text(root, 'HO_TEN_CHA'),
            'HO_TEN_ME': self._get_text(root, 'HO_TEN_ME'),
            'NGUOI_GIAM_HO': self._get_text(root, 'NGUOI_GIAM_HO'),
            'DON_VI': self._get_text(root, 'DON_VI'),
            'NGAY_VAO': self._get_text(root, 'NGAY_VAO'),
            'NGAY_RA': self._get_text(root, 'NGAY_RA'),
            'CHAN_DOAN_VAO': self._get_text(root, 'CHAN_DOAN_VAO'),
            'CHAN_DOAN_RV': self._get_text(root, 'CHAN_DOAN_RV'),
            'QT_BENHLY': self._get_text(root, 'QT_BENHLY'),
            'TOMTAT_KQ': self._get_text(root, 'TOMTAT_KQ'),
            'PP_DIEUTRI': self._get_text(root, 'PP_DIEUTRI'),
            'NGAY_SINHCON': self._get_text(root, 'NGAY_SINHCON'),
            'NGAY_CONCHET': self._get_text(root, 'NGAY_CONCHET'),
            'SO_CONCHET': self._get_int(root, 'SO_CONCHET'),
            'KET_QUA_DTRI': self._get_int(root, 'KET_QUA_DTRI'),
            'GHI_CHU': self._get_text(root, 'GHI_CHU'),
            'MA_TTDV': self._get_text(root, 'MA_TTDV'),
            'NGAY_CT': self._get_text(root, 'NGAY_CT'),
            'MA_THE_TAM': self._get_text(root, 'MA_THE_TAM'),
            'DU_PHONG': self._get_text(root, 'DU_PHONG')
        }
    
    # ========================================
    # XML9 - GIAY_CHUNG_SINH (OBJECT)
    # ========================================
    
    def _parse_xml9_chungsinh(self, root: ET.Element) -> Optional[Dict[str, Any]]:
        """XML9: GIAY_CHUNG_SINH - Giấy chứng sinh (SINGLE OBJECT)"""
        if root is None:
            return None
        
        return {
        'MA_LK': self._get_text(root, 'MA_LK'),
        'MA_BHXH_NND': self._get_text(root, 'MA_BHXH_NND'),
        'MA_THE_NND': self._get_text(root, 'MA_THE_NND'),
        'HO_TEN_NND': self._get_text(root, 'HO_TEN_NND'),
        'NGAYSINH_NND': self._get_text(root, 'NGAYSINH_NND'),
        'MA_DANTOC_NND': self._get_text(root, 'MA_DANTOC_NND'),
        'SO_CCCD_NND': self._get_text(root, 'SO_CCCD_NND'),
        'NGAYCAP_CCCD_NND': self._get_text(root, 'NGAYCAP_CCCD_NND'),
        'NOICAP_CCCD_NND': self._get_text(root, 'NOICAP_CCCD_NND'),
        'NOI_CU_TRU_NND': self._get_text(root, 'NOI_CU_TRU_NND'),
        'MA_QUOCTICH': self._get_text(root, 'MA_QUOCTICH'),
        'MATINH_CU_TRU': self._get_text(root, 'MATINH_CU_TRU'),
        'MAHUYEN_CU_TRU': self._get_text(root, 'MAHUYEN_CU_TRU'),
        'MAXA_CU_TRU': self._get_text(root, 'MAXA_CU_TRU'),
        'HO_TEN_CHA': self._get_text(root, 'HO_TEN_CHA'),
        'MA_THE_TAM': self._get_text(root, 'MA_THE_TAM'),
        'HO_TEN_CON': self._get_text(root, 'HO_TEN_CON'),
        'GIOI_TINH_CON': self._get_int(root, 'GIOI_TINH_CON'),
        'SO_CON': self._get_int(root, 'SO_CON'),
        'LAN_SINH': self._get_int(root, 'LAN_SINH'),
        'SO_CON_SONG': self._get_int(root, 'SO_CON_SONG'),
        'CAN_NANG_CON': self._get_float(root, 'CAN_NANG_CON'),
        'NGAY_SINH_CON': self._get_text(root, 'NGAY_SINH_CON'),
        'NOI_SINH_CON': self._get_text(root, 'NOI_SINH_CON'),
        'TINH_TRANG_CON': self._get_int(root, 'TINH_TRANG_CON'),
        'SINHCON_PHAUTHUAT': self._get_int(root, 'SINHCON_PHAUTHUAT'),
        'SINHCON_DUOI32TUAN': self._get_int(root, 'SINHCON_DUOI32TUAN'),
        'GHI_CHU': self._get_text(root, 'GHI_CHU'),
        'NGUOI_DO_DE': self._get_text(root, 'NGUOI_DO_DE'),
        'NGUOI_GHI_PHIEU': self._get_text(root, 'NGUOI_GHI_PHIEU'),
        'NGAY_CT': self._get_text(root, 'NGAY_CT'),
        'SO': self._get_text(root, 'SO'),
        'QUYEN_SO': self._get_text(root, 'QUYEN_SO'),
        'MA_TTDV': self._get_text(root, 'MA_TTDV'),
        'DU_PHONG': self._get_text(root, 'DU_PHONG')
    }
    # ========================================
    # XML10 - GIAY_NGHI_DUONG_THAI (OBJECT)
    # ========================================
    
    def _parse_xml10_nghithai(self, root: ET.Element) -> Optional[Dict[str, Any]]:
        """XML10: GIAY_NGHI_DUONG_THAI - Giấy nghỉ dưỡng thai (SINGLE OBJECT)"""
        if root is None:
            return None
        
        return {
        'MA_LK': self._get_text(root, 'MA_LK'),
        'SO_SERI': self._get_text(root, 'SO_SERI'),
        'SO_CT': self._get_text(root, 'SO_CT'),
        'SO_NGAY': self._get_int(root, 'SO_NGAY'),
        'DON_VI': self._get_text(root, 'DON_VI'),
        'CHAN_DOAN_RV': self._get_text(root, 'CHAN_DOAN_RV'),
        'TU_NGAY': self._get_text(root, 'TU_NGAY'),
        'DEN_NGAY': self._get_text(root, 'DEN_NGAY'),
        'MA_TTDV': self._get_text(root, 'MA_TTDV'),
        'TEN_BS': self._get_text(root, 'TEN_BS'),
        'MA_BS': self._get_text(root, 'MA_BS'),
        'NGAY_CT': self._get_text(root, 'NGAY_CT'),
        'DU_PHONG': self._get_text(root, 'DU_PHONG')
    }
    
    # ========================================
    # XML11 - GIAY_NGHI_VIEC_HUONG_BHXH (OBJECT)
    # ========================================
    
    def _parse_xml11_nghibhxh(self, root: ET.Element) -> Optional[Dict[str, Any]]:
        """XML11: GIAY_NGHI_VIEC_HUONG_BHXH - Giấy nghỉ việc hưởng BHXH (SINGLE OBJECT)"""
        if root is None:
            return None
        
        return {
        'MA_LK': self._get_text(root, 'MA_LK'),
        'SO_CT': self._get_text(root, 'SO_CT'),
        'SO_SERI': self._get_text(root, 'SO_SERI'),
        'SO_KCB': self._get_text(root, 'SO_KCB'),
        'DON_VI': self._get_text(root, 'DON_VI'),
        'MA_BHXH': self._get_text(root, 'MA_BHXH'),
        'MA_THE_BHYT': self._get_text(root, 'MA_THE_BHYT'),
        'CHAN_DOAN_RV': self._get_text(root, 'CHAN_DOAN_RV'),
        'PP_DIEUTRI': self._get_text(root, 'PP_DIEUTRI'),
        'MA_DINH_CHI_THAI': self._get_text(root, 'MA_DINH_CHI_THAI'),
        'NGUYENNHAN_DINHCHI': self._get_text(root, 'NGUYENNHAN_DINHCHI'),
        'TUOI_THAI': self._get_int(root, 'TUOI_THAI'),
        'SO_NGAY_NGHI': self._get_int(root, 'SO_NGAY_NGHI'),
        'TU_NGAY': self._get_text(root, 'TU_NGAY'),
        'DEN_NGAY': self._get_text(root, 'DEN_NGAY'),
        'HO_TEN_CHA': self._get_text(root, 'HO_TEN_CHA'),
        'HO_TEN_ME': self._get_text(root, 'HO_TEN_ME'),
        'MA_TTDV': self._get_text(root, 'MA_TTDV'),
        'MA_BS': self._get_text(root, 'MA_BS'),
        'NGAY_CT': self._get_text(root, 'NGAY_CT'),
        'MA_THE_TAM': self._get_text(root, 'MA_THE_TAM'),
        'MAU_SO': self._get_text(root, 'MAU_SO'),
        'DU_PHONG': self._get_text(root, 'DU_PHONG')
    }
    
    # ========================================
    # XML12 - DSACH_GIAM_DINH_Y_KHOA (ARRAY)
    # ========================================
    
    def _parse_xml12_giamdinh(self, root: ET.Element) -> List[Dict[str, Any]]:
        """XML12: DSACH_GIAM_DINH_Y_KHOA - Giám định y khoa (ARRAY)"""
        giamdinh_list = []
        
        for elem in root.findall('.//GIAM_DINH_Y_KHOA'):
            giamdinh = {
                'MA_LK': self._get_text(elem, 'MA_LK'),
                'STT': self._get_int(elem, 'STT'),
                'NGUOI_CHU_TRI': self._get_text(elem, 'NGUOI_CHU_TRI'),
                'CHUC_VU': self._get_text(elem, 'CHUC_VU'),
                'NGAY_HOP': self._get_text(elem, 'NGAY_HOP'),
                'HO_TEN': self._get_text(elem, 'HO_TEN'),
                'NGAY_SINH': self._get_text(elem, 'NGAY_SINH'),
                'SO_CCCD': self._get_text(elem, 'SO_CCCD'),
                'NGAY_CAP_CCCD': self._get_text(elem, 'NGAY_CAP_CCCD'),
                'NOI_CAP_CCCD': self._get_text(elem, 'NOI_CAP_CCCD'),
                'DIA_CHI': self._get_text(elem, 'DIA_CHI'),
                'MATINH_CU_TRU': self._get_text(elem, 'MATINH_CU_TRU'),
                'MAHUYEN_CU_TRU': self._get_text(elem, 'MAHUYEN_CU_TRU'),
                'MAXA_CU_TRU': self._get_text(elem, 'MAXA_CU_TRU'),
                'MA_BHXH': self._get_text(elem, 'MA_BHXH'),
                'MA_THE_BHYT': self._get_text(elem, 'MA_THE_BHYT'),
                'NGHE_NGHIEP': self._get_text(elem, 'NGHE_NGHIEP'),
                'DIEN_THOAI': self._get_text(elem, 'DIEN_THOAI'),
                'MA_DOI_TUONG': self._get_text(elem, 'MA_DOI_TUONG'),
                'KHAM_GIAM_DINH': self._get_text(elem, 'KHAM_GIAM_DINH'),
                'SO_BIEN_BAN': self._get_text(elem, 'SO_BIEN_BAN'),
                'TYLE_TTCT_CU': self._get_float(elem, 'TYLE_TTCT_CU'),
                'DANG_HUONG_CHE_DO': self._get_text(elem, 'DANG_HUONG_CHE_DO'),
                'NGAY_CHUNG_TU': self._get_text(elem, 'NGAY_CHUNG_TU'),
                'SO_GIAY_GIOI_THIEU': self._get_text(elem, 'SO_GIAY_GIOI_THIEU'),
                'NGAY_DE_NGHI': self._get_text(elem, 'NGAY_DE_NGHI'),
                'MA_DONVI': self._get_text(elem, 'MA_DONVI'),
                'GIOI_THIEU_CUA': self._get_text(elem, 'GIOI_THIEU_CUA'),
                'KET_QUA_KHAM': self._get_text(elem, 'KET_QUA_KHAM'),
                'SO_VAN_BAN_CAN_CU': self._get_text(elem, 'SO_VAN_BAN_CAN_CU'),
                'TYLE_TTCT_MOI': self._get_float(elem, 'TYLE_TTCT_MOI'),
                'TONG_TYLE_TTCT': self._get_float(elem, 'TONG_TYLE_TTCT'),
                'DANG_KHUYETTAT': self._get_text(elem, 'DANG_KHUYETTAT'),
                'MUC_DO_KHUYETTAT': self._get_text(elem, 'MUC_DO_KHUYETTAT'),
                'DE_NGHI': self._get_text(elem, 'DE_NGHI'),
                'DUOC_XACDINH': self._get_text(elem, 'DUOC_XACDINH'),
                'DU_PHONG': self._get_text(elem, 'DU_PHONG')
            }
            giamdinh_list.append(giamdinh)
        
        return giamdinh_list
    
    # ========================================
    # XML13 - DSACH_GIAY_CHUYEN_TUYEN (ARRAY)
    # ========================================
    
    def _parse_xml13_chuyentuyen(self, root: ET.Element) -> List[Dict[str, Any]]:
        """XML13: DSACH_GIAY_CHUYEN_TUYEN - Giấy chuyển tuyến (ARRAY)"""
        chuyentuyen_list = []
        
        for elem in root.findall('.//GIAY_CHUYEN_TUYEN'):
            chuyentuyen = {
                'MA_LK': self._get_text(elem, 'MA_LK'),
                'STT': self._get_int(elem, 'STT'),
                'SO_HOSO': self._get_text(elem, 'SO_HOSO'),
                'SO_CHUYENTUYEN': self._get_text(elem, 'SO_CHUYENTUYEN'),
                'GIAY_CHUYEN_TUYEN': self._get_text(elem, 'GIAY_CHUYEN_TUYEN'),
                'MA_CSKCB': self._get_text(elem, 'MA_CSKCB'),
                'MA_NOI_DI': self._get_text(elem, 'MA_NOI_DI'),
                'MA_NOI_DEN': self._get_text(elem, 'MA_NOI_DEN'),
                'HO_TEN': self._get_text(elem, 'HO_TEN'),
                'NGAY_SINH': self._get_text(elem, 'NGAY_SINH'),
                'GIOI_TINH': self._get_int(elem, 'GIOI_TINH'),
                'MA_QUOCTICH': self._get_text(elem, 'MA_QUOCTICH'),
                'MA_DANTOC': self._get_text(elem, 'MA_DANTOC'),
                'MA_NGHE_NGHIEP': self._get_text(elem, 'MA_NGHE_NGHIEP'),
                'DIA_CHI': self._get_text(elem, 'DIA_CHI'),
                'MA_THE_BHYT': self._get_text(elem, 'MA_THE_BHYT'),
                'GT_THE_DEN': self._get_text(elem, 'GT_THE_DEN'),
                'NGAY_VAO': self._get_text(elem, 'NGAY_VAO'),
                'NGAY_VAO_NOI_TRU': self._get_text(elem, 'NGAY_VAO_NOI_TRU'),
                'NGAY_RA': self._get_text(elem, 'NGAY_RA'),
                'DAU_HIEU_LS': self._get_text(elem, 'DAU_HIEU_LS'),
                'CHAN_DOAN_RV': self._get_text(elem, 'CHAN_DOAN_RV'),
                'QT_BENHLY': self._get_text(elem, 'QT_BENHLY'),
                'TOMTAT_KQ': self._get_text(elem, 'TOMTAT_KQ'),
                'PP_DIEUTRI': self._get_text(elem, 'PP_DIEUTRI'),
                'MA_BENH_CHINH': self._get_text(elem, 'MA_BENH_CHINH'),
                'MA_BENH_KT': self._get_text(elem, 'MA_BENH_KT'),
                'MA_BENH_YHCT': self._get_text(elem, 'MA_BENH_YHCT'),
                'TEN_DICH_VU': self._get_text(elem, 'TEN_DICH_VU'),
                'TEN_THUOC': self._get_text(elem, 'TEN_THUOC'),
                'PP_DIEU_TRI': self._get_text(elem, 'PP_DIEU_TRI'),
                'MA_LOAI_RV': self._get_text(elem, 'MA_LOAI_RV'),
                'MA_LYDO_CT': self._get_text(elem, 'MA_LYDO_CT'),
                'HUONG_DIEU_TRI': self._get_text(elem, 'HUONG_DIEU_TRI'),
                'PHUONGTIEN_VC': self._get_text(elem, 'PHUONGTIEN_VC'),
                'HOTEN_NGUOI_HT': self._get_text(elem, 'HOTEN_NGUOI_HT'),
                'CHUCDANH_NGUOI_HT': self._get_text(elem, 'CHUCDANH_NGUOI_HT'),
                'MA_BAC_SI': self._get_text(elem, 'MA_BAC_SI'),
                'MA_TTDV': self._get_text(elem, 'MA_TTDV'),
                'DU_PHONG': self._get_text(elem, 'DU_PHONG')
            }
            chuyentuyen_list.append(chuyentuyen)
        
        return chuyentuyen_list
    
    # ========================================
    # XML14 - DSACH_GIAY_HEN_KHAM_LAI (ARRAY)
    # ========================================
    
    def _parse_xml14_henkham(self, root: ET.Element) -> List[Dict[str, Any]]:
        """XML14: DSACH_GIAY_HEN_KHAM_LAI - Giấy hẹn khám lại (ARRAY)"""
        henkham_list = []
        
        for elem in root.findall('.//GIAY_HEN_KHAM_LAI'):
            henkham = {
                'MA_LK': self._get_text(elem, 'MA_LK'),
                'STT': self._get_int(elem, 'STT'),
                'SO_GIAYHEN_KL': self._get_text(elem, 'SO_GIAYHEN_KL'),
                'MA_CSKCB': self._get_text(elem, 'MA_CSKCB'),
                'HO_TEN': self._get_text(elem, 'HO_TEN'),
                'NGAY_SINH': self._get_text(elem, 'NGAY_SINH'),
                'GIOI_TINH': self._get_int(elem, 'GIOI_TINH'),
                'DIA_CHI': self._get_text(elem, 'DIA_CHI'),
                'MA_THE_BHYT': self._get_text(elem, 'MA_THE_BHYT'),
                'GT_THE_DEN': self._get_text(elem, 'GT_THE_DEN'),
                'NGAY_VAO': self._get_text(elem, 'NGAY_VAO'),
                'NGAY_VAO_NOI_TRU': self._get_text(elem, 'NGAY_VAO_NOI_TRU'),
                'NGAY_RA': self._get_text(elem, 'NGAY_RA'),
                'NGAY_HEN_KL': self._get_text(elem, 'NGAY_HEN_KL'),
                'CHAN_DOAN_RV': self._get_text(elem, 'CHAN_DOAN_RV'),
                'MA_BENH_CHINH': self._get_text(elem, 'MA_BENH_CHINH'),
                'MA_BENH_KT': self._get_text(elem, 'MA_BENH_KT'),
                'MA_BENH_YHCT': self._get_text(elem, 'MA_BENH_YHCT'),
                'MA_DOITUONG_KCB': self._get_text(elem, 'MA_DOITUONG_KCB'),
                'MA_BAC_SI': self._get_text(elem, 'MA_BAC_SI'),
                'MA_TTDV': self._get_text(elem, 'MA_TTDV'),
                'NGAY_CT': self._get_text(elem, 'NGAY_CT'),
                'DU_PHONG': self._get_text(elem, 'DU_PHONG')
            }
            henkham_list.append(henkham)
        
        return henkham_list
    
    # ========================================
    # XML15 - DSACH_THONG_TIN_LAO (ARRAY)
    # ========================================
    
    def _parse_xml15_lao(self, root: ET.Element) -> List[Dict[str, Any]]:
        """XML15: DSACH_THONG_TIN_LAO - Thông tin bệnh Lao (ARRAY)"""
        lao_list = []
        
        for elem in root.findall('.//THONG_TIN_LAO'):
            lao = {
                'MA_LK': self._get_text(elem, 'MA_LK'),
                'STT': self._get_int(elem, 'STT'),
                'MA_BN': self._get_text(elem, 'MA_BN'),
                'HO_TEN': self._get_text(elem, 'HO_TEN'),
                'SO_CCCD': self._get_text(elem, 'SO_CCCD'),
                'PHANLOAI_LAO_VITRI': self._get_text(elem, 'PHANLOAI_LAO_VITRI'),
                'PHANLOAI_LAO_TS': self._get_text(elem, 'PHANLOAI_LAO_TS'),
                'PHANLOAI_LAO_HIV': self._get_text(elem, 'PHANLOAI_LAO_HIV'),
                'PHANLOAI_LAO_VK': self._get_text(elem, 'PHANLOAI_LAO_VK'),
                'PHANLOAI_LAO_KT': self._get_text(elem, 'PHANLOAI_LAO_KT'),
                'LOAI_DTRI_LAO': self._get_text(elem, 'LOAI_DTRI_LAO'),
                'NGAYBD_DTRI_LAO': self._get_text(elem, 'NGAYBD_DTRI_LAO'),
                'PHACDO_DTRI_LAO': self._get_text(elem, 'PHACDO_DTRI_LAO'),
                'NGAYKT_DTRI_LAO': self._get_text(elem, 'NGAYKT_DTRI_LAO'),
                'KET_QUA_DTRI_LAO': self._get_text(elem, 'KET_QUA_DTRI_LAO'),
                'MA_CSKCB': self._get_text(elem, 'MA_CSKCB'),
                'NGAYKD_HIV': self._get_text(elem, 'NGAYKD_HIV'),
                'BDDT_ARV': self._get_text(elem, 'BDDT_ARV'),
                'NGAY_BAT_DAU_DT_CTX': self._get_text(elem, 'NGAY_BAT_DAU_DT_CTX'),
                'DU_PHONG': self._get_text(elem, 'DU_PHONG')
            }
            lao_list.append(lao)
        
        return lao_list
    
    # ========================================
    # Helper Methods
    # ========================================
    
    def _get_text(self, element: ET.Element, path: str, default: str = '') -> str:
        """Get text value"""
        if element is None:
            return default
        
        found = element.find(path)
        if found is not None and found.text:
            return found.text.strip()
        
        return default
    
    def _get_int(self, element: ET.Element, path: str, default: int = 0) -> int:
        """Get integer value"""
        text = self._get_text(element, path)
        try:
            return int(float(text)) if text else default
        except:
            return default
    
    def _get_float(self, element: ET.Element, path: str, default: float = 0.0) -> float:
        """Get float value"""
        text = self._get_text(element, path)
        try:
            return float(text) if text else default
        except:
            return default
    
    def _count_xml_types(self, records: List[Dict[str, Any]]) -> Dict[str, int]:
        """Count XML types"""
        counts = {}
        xml_keys = [
            'TONG_HOP', 'DSACH_CHI_TIET_THUOC', 'DSACH_CHI_TIET_DVKT',
            'DSACH_CHI_TIET_CLS', 'DSACH_CHI_TIET_DIEN_BIEN_BENH'
        ]
        
        for key in xml_keys:
            count = sum(1 for r in records if key in r and r[key])
            if count > 0:
                counts[key] = count
        
        return counts
