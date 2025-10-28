"""
Complete BHYT Data Transformer - Parse đầy đủ TẤT CẢ các bảng XML
Theo Quyết định 4750/QĐ-BYT
"""

import xml.etree.ElementTree as ET
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging


class CompleteBHYTTransformer:
    """
    Transformer đầy đủ cho tất cả các bảng XML BHYT
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.encoding = 'UTF-8'
    
    def transform_complete_xml(self, xml_content: str) -> Dict[str, Any]:
        """
        Parse đầy đủ TẤT CẢ các bảng XML BHYT theo QĐ 4750
        """
        self.logger.info("Starting complete BHYT XML transformation")
        
        try:
            # Remove BOM nếu có
            if xml_content.startswith('\ufeff'):
                xml_content = xml_content[1:]
            
            root = ET.fromstring(xml_content.encode(self.encoding))
        except Exception as e:
            self.logger.error(f"XML parsing failed: {str(e)}")
            raise ValueError(f"Invalid XML content: {str(e)}")
        
        # Parse toàn bộ cấu trúc
        dto = {
            'resourceType': 'BHYTClaimRecordComplete',
            'standard': 'QD_4750_BYT_v2.0',
            'version': '2.0',
            
            # HEADER - Thông tin file
            'header': self._parse_header(root),
            
            # BẢNG 1: Tổng hợp khám chữa bệnh
            'tonghop': self._parse_tonghop(root),
            
            # BẢNG 2: Chi tiết thuốc
            'chitiet_thuoc': self._parse_chitiet_thuoc(root),
            
            # BẢNG 3: Chi tiết DVKT và VTYT
            'chitiet_dvkt': self._parse_chitiet_dvkt(root),
            
            # BẢNG 4: Chi tiết dịch vụ cận lâm sàng
            'chitiet_cls': self._parse_chitiet_cls(root),
            
            # BẢNG 5: Diễn biến lâm sàng
            'dienbienlamsang': self._parse_dienbienlamsang(root),
            
            # BẢNG BỔ SUNG: Chẩn đoán hình ảnh
            'chandoan_hinhanh': self._parse_chandoan_hinhanh(root),
            
            # BẢNG BỔ SUNG: Giải phẫu bệnh
            'giaiphaubenh': self._parse_giaiphaubenh(root),
            
            # BẢNG BỔ SUNG: Thủ thuật, phẫu thuật
            'thuthuat_phauth': self._parse_thuthuat_phauthuat(root),
            
            # BẢNG BỔ SUNG: Chỉ định giường bệnh
            'giuong_benh': self._parse_giuong_benh(root),
            
            # BẢNG BỔ SUNG: Truyền máu
            'truyen_mau': self._parse_truyen_mau(root),
            
            # BẢNG BỔ SUNG: Vật tư thay thế
            'vtyt_thaythe': self._parse_vtyt_thaythe(root),
            
            # BẢNG BỔ SUNG: Dịch vụ kỹ thuật cao
            'dvkt_cao': self._parse_dvkt_cao(root),
            
            # Metadata
            'metadata': {
                'transformedAt': datetime.utcnow().isoformat(),
                'transformer': 'CompleteBHYTTransformer_v2.0',
                'recordCount': self._count_records(root)
            }
        }
        
        self.logger.info(f"Complete transformation finished. MA_LK: {dto['tonghop'].get('MA_LK')}")
        return dto
    
    def _parse_header(self, root: ET.Element) -> Dict[str, Any]:
        """Parse header information"""
        return {
            'LOAIHOSO': self._get_text(root, './/LOAIHOSO'),
            'MACSKCB': self._get_text(root, './/MACSKCB'),
            'NGAYLAP': self._get_text(root, './/NGAYLAP'),
            'SOLUONGHOSO': self._get_int(root, './/SOLUONGHOSO'),
            'DANHSACHHOSO': self._get_text(root, './/DANHSACHHOSO'),
            'CHUCDANH_NGUOILAP': self._get_text(root, './/CHUCDANH_NGUOILAP'),
            'HOTEN_NGUOILAP': self._get_text(root, './/HOTEN_NGUOILAP'),
            'NGUOIGIAOHOSO': self._get_text(root, './/NGUOIGIAOHOSO'),
            'NGAYNGAOHOSO': self._get_text(root, './/NGAYNGAOHOSO'),
            'NGUOINHAN': self._get_text(root, './/NGUOINHAN'),
            'NGAYNHANHOSO': self._get_text(root, './/NGAYNHANHOSO')
        }
    
    def _parse_tonghop(self, root: ET.Element) -> Dict[str, Any]:
        """
        BẢNG 1: Chi tiết tổng hợp khám chữa bệnh (ĐẦY ĐỦ)
        """
        elem = self._find_tonghop_element(root)
        
        return {
            # Mã liên kết và STT
            'MA_LK': self._get_text(elem, 'MA_LK'),
            'STT': self._get_text(elem, 'STT'),
            
            # Thông tin hành chính bệnh nhân
            'MA_BN': self._get_text(elem, 'MA_BN'),
            'HO_TEN': self._get_text(elem, 'HO_TEN'),
            'SO_CCCD': self._get_text(elem, 'SO_CCCD'),
            'NGAY_SINH': self._get_text(elem, 'NGAY_SINH'),
            'GIOI_TINH': self._get_int(elem, 'GIOI_TINH'),
            'NHOM_MAU': self._get_text(elem, 'NHOM_MAU'),
            'DIA_CHI': self._get_text(elem, 'DIA_CHI'),
            'MA_TINH': self._get_text(elem, 'MA_TINH'),
            'MA_HUYEN': self._get_text(elem, 'MA_HUYEN'),
            'MA_XA': self._get_text(elem, 'MA_XA'),
            'DIA_CHI_CHITIET': self._get_text(elem, 'DIA_CHI_CHITIET'),
            'MA_KV': self._get_text(elem, 'MA_KV'),
            
            # Thông tin thẻ BHYT
            'MA_THE': self._get_text(elem, 'MA_THE'),
            'MA_DKBD': self._get_text(elem, 'MA_DKBD'),
            'GT_THE_TU': self._get_text(elem, 'GT_THE_TU'),
            'GT_THE_DEN': self._get_text(elem, 'GT_THE_DEN'),
            'MIEN_CUNG_CT': self._get_int(elem, 'MIEN_CUNG_CT'),
            'TEN_BENH': self._get_text(elem, 'TEN_BENH'),
            'MA_BENH': self._get_text(elem, 'MA_BENH'),
            'MA_BENHKHAC': self._get_text(elem, 'MA_BENHKHAC'),
            'MA_BENH_YHCT': self._get_text(elem, 'MA_BENH_YHCT'),
            'MA_DOITUONG_KCB': self._get_text(elem, 'MA_DOITUONG_KCB'),
            
            # Thông tin KCB
            'MA_LYDO_VVIEN': self._get_text(elem, 'MA_LYDO_VVIEN'),
            'MA_NOI_CHUYEN': self._get_text(elem, 'MA_NOI_CHUYEN'),
            'MA_TAI_NAN': self._get_text(elem, 'MA_TAI_NAN'),
            'NGAY_VAO': self._get_text(elem, 'NGAY_VAO'),
            'NGAY_VAO_NOI_TRU': self._get_text(elem, 'NGAY_VAO_NOI_TRU'),
            'NGAY_RA': self._get_text(elem, 'NGAY_RA'),
            'GIAY_CHUYEN_TUYEN': self._get_text(elem, 'GIAY_CHUYEN_TUYEN'),
            'SO_NGAY_DTRI': self._get_int(elem, 'SO_NGAY_DTRI'),
            'PP_DIEUTRI': self._get_text(elem, 'PP_DIEUTRI'),
            'KET_QUA_DTRI': self._get_int(elem, 'KET_QUA_DTRI'),
            'TINH_TRANG_RV': self._get_int(elem, 'TINH_TRANG_RV'),
            'NGAY_TTOAN': self._get_text(elem, 'NGAY_TTOAN'),
            
            # Thông tin cơ sở KCB
            'MA_LOAI_KCB': self._get_int(elem, 'MA_LOAI_KCB'),
            'MA_KHOA': self._get_text(elem, 'MA_KHOA'),
            'MA_CSKCB': self._get_text(elem, 'MA_CSKCB'),
            'MA_KHUVUC': self._get_text(elem, 'MA_KHUVUC'),
            'MA_PTTT_QT': self._get_text(elem, 'MA_PTTT_QT'),
            'CAN_NANG': self._get_float(elem, 'CAN_NANG'),
            
            # Chi phí tổng hợp
            'T_TONGCHI': self._get_float(elem, 'T_TONGCHI'),
            'T_TONGCHI_BV': self._get_float(elem, 'T_TONGCHI_BV'),
            'T_TONGCHI_BH': self._get_float(elem, 'T_TONGCHI_BH'),
            'T_NGOAIDM': self._get_float(elem, 'T_NGOAIDM'),
            'T_BHTT': self._get_float(elem, 'T_BHTT'),
            'T_BNCCT': self._get_float(elem, 'T_BNCCT'),
            'T_BNTT': self._get_float(elem, 'T_BNTT'),
            'T_NGUONKHAC': self._get_float(elem, 'T_NGUONKHAC'),
            
            # Chi phí chi tiết theo loại
            'T_KHAM': self._get_float(elem, 'T_KHAM'),
            'T_CDHA': self._get_float(elem, 'T_CDHA'),
            'T_XETNGHIEM': self._get_float(elem, 'T_XETNGHIEM'),
            'T_THUOC': self._get_float(elem, 'T_THUOC'),
            'T_MAU': self._get_float(elem, 'T_MAU'),
            'T_PTTT': self._get_float(elem, 'T_PTTT'),
            'T_VTYT': self._get_float(elem, 'T_VTYT'),
            'T_DVKT_TYLE': self._get_float(elem, 'T_DVKT_TYLE'),
            'T_THUOC_TYLE': self._get_float(elem, 'T_THUOC_TYLE'),
            'T_KHAM_TYLE': self._get_float(elem, 'T_KHAM_TYLE'),
            'T_GIUONG': self._get_float(elem, 'T_GIUONG'),
            'T_VCHUYEN': self._get_float(elem, 'T_VCHUYEN'),
            'T_BNTT_VTYT': self._get_float(elem, 'T_BNTT_VTYT'),
            
            # Thông tin bác sĩ
            'MA_BACSI_DIEUTRI': self._get_text(elem, 'MA_BACSI_DIEUTRI'),
            'TEN_BACSI_DIEUTRI': self._get_text(elem, 'TEN_BACSI_DIEUTRI'),
            'MA_BACSI_KETLUAN': self._get_text(elem, 'MA_BACSI_KETLUAN'),
            'TEN_BACSI_KETLUAN': self._get_text(elem, 'TEN_BACSI_KETLUAN'),
            
            # Thông tin bổ sung
            'MA_HSBA': self._get_text(elem, 'MA_HSBA'),
            'MA_TTDV': self._get_text(elem, 'MA_TTDV'),
            'DU_PHONG': self._get_text(elem, 'DU_PHONG'),
            'NAM_QT': self._get_int(elem, 'NAM_QT'),
            'THANG_QT': self._get_int(elem, 'THANG_QT'),
            'MA_LOAI_RV': self._get_text(elem, 'MA_LOAI_RV'),
            'MA_LOAI_RAVIEN': self._get_text(elem, 'MA_LOAI_RAVIEN'),
            'SO_HDON': self._get_text(elem, 'SO_HDON'),
            'DON_THUOC_HDON': self._get_text(elem, 'DON_THUOC_HDON'),
            'GHI_CHU': self._get_text(elem, 'GHI_CHU')
        }
    
    def _parse_chitiet_thuoc(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        BẢNG 2: Chi tiết thuốc (ĐẦY ĐỦ TẤT CẢ TRƯỜNG)
        """
        thuoc_list = []
        
        for thuoc_elem in root.findall('.//CHI_TIET_THUOC') or root.findall('.//THUOC'):
            thuoc = {
                'MA_LK': self._get_text(thuoc_elem, 'MA_LK'),
                'STT': self._get_int(thuoc_elem, 'STT'),
                'MA_THUOC': self._get_text(thuoc_elem, 'MA_THUOC'),
                'MA_PP_CHEBIEN': self._get_text(thuoc_elem, 'MA_PP_CHEBIEN'),
                'MA_CSKCB_THUOC': self._get_text(thuoc_elem, 'MA_CSKCB_THUOC'),
                'MA_NHOM': self._get_int(thuoc_elem, 'MA_NHOM'),
                'TEN_THUOC': self._get_text(thuoc_elem, 'TEN_THUOC'),
                'DON_VI_TINH': self._get_text(thuoc_elem, 'DON_VI_TINH'),
                'HAM_LUONG': self._get_text(thuoc_elem, 'HAM_LUONG'),
                'DUONG_DUNG': self._get_text(thuoc_elem, 'DUONG_DUNG'),
                'DANG_BAO_CHE': self._get_text(thuoc_elem, 'DANG_BAO_CHE'),
                'LIEU_DUNG': self._get_text(thuoc_elem, 'LIEU_DUNG'),
                'CACH_DUNG': self._get_text(thuoc_elem, 'CACH_DUNG'),
                'SO_DANG_KY': self._get_text(thuoc_elem, 'SO_DANG_KY'),
                'TT_THAU': self._get_text(thuoc_elem, 'TT_THAU'),
                'PHAM_VI': self._get_int(thuoc_elem, 'PHAM_VI'),
                'TYLE_TT_DV': self._get_float(thuoc_elem, 'TYLE_TT_DV'),
                'SO_LUONG': self._get_float(thuoc_elem, 'SO_LUONG'),
                'DON_GIA': self._get_float(thuoc_elem, 'DON_GIA'),
                'TYLE_TT_BH': self._get_float(thuoc_elem, 'TYLE_TT_BH'),
                'THANH_TIEN': self._get_float(thuoc_elem, 'THANH_TIEN'),
                'MUC_HUONG': self._get_int(thuoc_elem, 'MUC_HUONG'),
                'T_TRANTT': self._get_float(thuoc_elem, 'T_TRANTT'),
                'T_BHTT': self._get_float(thuoc_elem, 'T_BHTT'),
                'T_BNCCT': self._get_float(thuoc_elem, 'T_BNCCT'),
                'T_BNTT': self._get_float(thuoc_elem, 'T_BNTT'),
                'T_NGUONKHAC': self._get_float(thuoc_elem, 'T_NGUONKHAC'),
                'MA_KHOA': self._get_text(thuoc_elem, 'MA_KHOA'),
                'MA_BAC_SI': self._get_text(thuoc_elem, 'MA_BAC_SI'),
                'MA_DICH_VU': self._get_text(thuoc_elem, 'MA_DICH_VU'),
                'MA_BENH': self._get_text(thuoc_elem, 'MA_BENH'),
                'NGAY_YL': self._get_text(thuoc_elem, 'NGAY_YL'),
                'NGAY_TH_YL': self._get_text(thuoc_elem, 'NGAY_TH_YL'),
                'MA_PTTT': self._get_text(thuoc_elem, 'MA_PTTT'),
                'NGUON_KHAC': self._get_float(thuoc_elem, 'NGUON_KHAC'),
                'SO_NGAY': self._get_int(thuoc_elem, 'SO_NGAY'),
                'SO_LUONG_KEDON': self._get_float(thuoc_elem, 'SO_LUONG_KEDON'),
                'LOAI_THUOC': self._get_int(thuoc_elem, 'LOAI_THUOC'),
                'TEN_DIEU_TRI': self._get_text(thuoc_elem, 'TEN_DIEU_TRI'),
                'NGUOI_THUC_HIEN': self._get_text(thuoc_elem, 'NGUOI_THUC_HIEN'),
                'DU_PHONG': self._get_text(thuoc_elem, 'DU_PHONG')
            }
            thuoc_list.append(thuoc)
        
        return thuoc_list
    
    def _parse_chitiet_dvkt(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        BẢNG 3: Chi tiết DVKT và VTYT (HOÀN CHỈNH)
        """
        dvkt_list = []
        
        for dvkt_elem in root.findall('.//CHI_TIET_DVKT') or root.findall('.//DVKT'):
            dvkt = {
                'MA_LK': self._get_text(dvkt_elem, 'MA_LK'),
                'STT': self._get_int(dvkt_elem, 'STT'),
                'MA_DICH_VU': self._get_text(dvkt_elem, 'MA_DICH_VU'),
                'MA_VAT_TU': self._get_text(dvkt_elem, 'MA_VAT_TU'),
                'MA_NHOM': self._get_int(dvkt_elem, 'MA_NHOM'),
                'GOI_VTYT': self._get_text(dvkt_elem, 'GOI_VTYT'),
                'TEN_VAT_TU': self._get_text(dvkt_elem, 'TEN_VAT_TU'),
                'TEN_DICH_VU': self._get_text(dvkt_elem, 'TEN_DICH_VU'),
                'MA_XANG_DAU': self._get_text(dvkt_elem, 'MA_XANG_DAU'),
                'DON_VI_TINH': self._get_text(dvkt_elem, 'DON_VI_TINH'),
                'PHAM_VI': self._get_int(dvkt_elem, 'PHAM_VI'),
                'SO_LUONG': self._get_float(dvkt_elem, 'SO_LUONG'),
                'DON_GIA_BV': self._get_float(dvkt_elem, 'DON_GIA_BV'),
                'DON_GIA_BH': self._get_float(dvkt_elem, 'DON_GIA_BH'),
                'TYLE_TT_DV': self._get_float(dvkt_elem, 'TYLE_TT_DV'),
                'TYLE_TT_BH': self._get_float(dvkt_elem, 'TYLE_TT_BH'),
                'TT_THAU': self._get_text(dvkt_elem, 'TT_THAU'),
                'THANH_TIEN_BV': self._get_float(dvkt_elem, 'THANH_TIEN_BV'),
                'THANH_TIEN_BH': self._get_float(dvkt_elem, 'THANH_TIEN_BH'),
                'T_TRANTT': self._get_float(dvkt_elem, 'T_TRANTT'),
                'MUC_HUONG': self._get_int(dvkt_elem, 'MUC_HUONG'),
                'T_BHTT': self._get_float(dvkt_elem, 'T_BHTT'),
                'T_BNCCT': self._get_float(dvkt_elem, 'T_BNCCT'),
                'T_BNTT': self._get_float(dvkt_elem, 'T_BNTT'),
                'T_NGUONKHAC': self._get_float(dvkt_elem, 'T_NGUONKHAC'),
                'MA_KHOA': self._get_text(dvkt_elem, 'MA_KHOA'),
                'MA_GIUONG': self._get_text(dvkt_elem, 'MA_GIUONG'),
                'MA_BAC_SI': self._get_text(dvkt_elem, 'MA_BAC_SI'),
                'MA_BENH': self._get_text(dvkt_elem, 'MA_BENH'),
                'NGAY_YL': self._get_text(dvkt_elem, 'NGAY_YL'),
                'NGAY_TH_YL': self._get_text(dvkt_elem, 'NGAY_TH_YL'),
                'NGAY_KQ': self._get_text(dvkt_elem, 'NGAY_KQ'),
                'MA_PTTT': self._get_text(dvkt_elem, 'MA_PTTT'),
                'MA_MAYDO': self._get_text(dvkt_elem, 'MA_MAYDO'),
                'TEN_MAYDO': self._get_text(dvkt_elem, 'TEN_MAYDO'),
                'MA_HIEU': self._get_text(dvkt_elem, 'MA_HIEU'),
                'PHAN_LOAI_PTTT': self._get_text(dvkt_elem, 'PHAN_LOAI_PTTT'),
                'VET_THUONG_TP': self._get_text(dvkt_elem, 'VET_THUONG_TP'),
                'PP_VO_CAM': self._get_text(dvkt_elem, 'PP_VO_CAM'),
                'VI_TRI_TH_DVKT': self._get_text(dvkt_elem, 'VI_TRI_TH_DVKT'),
                'MA_MAY_XN': self._get_text(dvkt_elem, 'MA_MAY_XN'),
                'SO_LUONG_NHAN_VIEN': self._get_int(dvkt_elem, 'SO_LUONG_NHAN_VIEN'),
                'THOI_GIAN_THUC_HIEN': self._get_float(dvkt_elem, 'THOI_GIAN_THUC_HIEN'),
                'SO_NGAY': self._get_int(dvkt_elem, 'SO_NGAY'),
                'SO_LAN': self._get_int(dvkt_elem, 'SO_LAN'),
                'KET_QUA': self._get_text(dvkt_elem, 'KET_QUA'),
                'KET_LUAN': self._get_text(dvkt_elem, 'KET_LUAN'),
                'NGUOI_THUC_HIEN': self._get_text(dvkt_elem, 'NGUOI_THUC_HIEN'),
                'GHI_CHU': self._get_text(dvkt_elem, 'GHI_CHU'),
                'NGAY_HIEU_LUC': self._get_text(dvkt_elem, 'NGAY_HIEU_LUC'),
                'NGAY_HET_HIEU_LUC': self._get_text(dvkt_elem, 'NGAY_HET_HIEU_LUC'),
                'DON_GIA': self._get_float(dvkt_elem, 'DON_GIA'),
                'THANH_TIEN': self._get_float(dvkt_elem, 'THANH_TIEN'),
                'MA_CSKCB': self._get_text(dvkt_elem, 'MA_CSKCB'),
                'DU_PHONG': self._get_text(dvkt_elem, 'DU_PHONG')
            }
            dvkt_list.append(dvkt)
        
        return dvkt_list
    
    def _parse_chitiet_cls(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        BẢNG 4: Chi tiết cận lâm sàng (XÉT NGHIỆM)
        """
        cls_list = []
        
        for cls_elem in root.findall('.//CHI_TIET_CLS') or root.findall('.//CAN_LAM_SANG'):
            cls = {
                'MA_LK': self._get_text(cls_elem, 'MA_LK'),
                'STT': self._get_int(cls_elem, 'STT'),
                'MA_DICH_VU': self._get_text(cls_elem, 'MA_DICH_VU'),
                'MA_CHI_SO': self._get_text(cls_elem, 'MA_CHI_SO'),
                'TEN_CHI_SO': self._get_text(cls_elem, 'TEN_CHI_SO'),
                'GIA_TRI': self._get_text(cls_elem, 'GIA_TRI'),
                'DON_VI_DO': self._get_text(cls_elem, 'DON_VI_DO'),
                'CSBT_THAP': self._get_text(cls_elem, 'CSBT_THAP'),
                'CSBT_CAO': self._get_text(cls_elem, 'CSBT_CAO'),
                'GIA_TRI_MIN': self._get_text(cls_elem, 'GIA_TRI_MIN'),
                'GIA_TRI_MAX': self._get_text(cls_elem, 'GIA_TRI_MAX'),
                'MA_MAY': self._get_text(cls_elem, 'MA_MAY'),
                'TEN_MAY': self._get_text(cls_elem, 'TEN_MAY'),
                'MO_TA': self._get_text(cls_elem, 'MO_TA'),
                'KET_LUAN': self._get_text(cls_elem, 'KET_LUAN'),
                'NGAY_KQ': self._get_text(cls_elem, 'NGAY_KQ'),
                'NGUOI_THUC_HIEN': self._get_text(cls_elem, 'NGUOI_THUC_HIEN'),
                'MA_BENH': self._get_text(cls_elem, 'MA_BENH'),
                'MA_KHOA': self._get_text(cls_elem, 'MA_KHOA'),
                'TEN_KHOA': self._get_text(cls_elem, 'TEN_KHOA'),
                'DU_PHONG': self._get_text(cls_elem, 'DU_PHONG')
            }
            cls_list.append(cls)
        
        return cls_list
    
    def _parse_dienbienlamsang(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        BẢNG 5: Diễn biến lâm sàng
        """
        dbls_list = []
        
        for dbls_elem in root.findall('.//DIEN_BIEN_LAM_SANG') or root.findall('.//DBLS'):
            dbls = {
                'MA_LK': self._get_text(dbls_elem, 'MA_LK'),
                'STT': self._get_int(dbls_elem, 'STT'),
                'DIEN_BIEN': self._get_text(dbls_elem, 'DIEN_BIEN'),
                'HOI_CHAN': self._get_text(dbls_elem, 'HOI_CHAN'),
                'PHAU_THUAT': self._get_text(dbls_elem, 'PHAU_THUAT'),
                'NGAY_YL': self._get_text(dbls_elem, 'NGAY_YL'),
                'NGUOI_THUC_HIEN': self._get_text(dbls_elem, 'NGUOI_THUC_HIEN'),
                'DU_PHONG': self._get_text(dbls_elem, 'DU_PHONG'),
                'GHI_CHU': self._get_text(dbls_elem, 'GHI_CHU')
            }
            dbls_list.append(dbls)
        
        return dbls_list
    
    def _parse_chandoan_hinhanh(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        BẢNG BỔ SUNG: Chẩn đoán hình ảnh
        """
        cdha_list = []
        
        for cdha_elem in root.findall('.//CHAN_DOAN_HINH_ANH') or root.findall('.//CDHA'):
            cdha = {
                'MA_LK': self._get_text(cdha_elem, 'MA_LK'),
                'STT': self._get_int(cdha_elem, 'STT'),
                'MA_DICH_VU': self._get_text(cdha_elem, 'MA_DICH_VU'),
                'TEN_DICH_VU': self._get_text(cdha_elem, 'TEN_DICH_VU'),
                'NGAY_CHI_DINH': self._get_text(cdha_elem, 'NGAY_CHI_DINH'),
                'NGAY_KQ': self._get_text(cdha_elem, 'NGAY_KQ'),
                'MO_TA': self._get_text(cdha_elem, 'MO_TA'),
                'KET_LUAN': self._get_text(cdha_elem, 'KET_LUAN'),
                'BS_CHI_DINH': self._get_text(cdha_elem, 'BS_CHI_DINH'),
                'BS_THUC_HIEN': self._get_text(cdha_elem, 'BS_THUC_HIEN'),
                'BS_DOC_KQ': self._get_text(cdha_elem, 'BS_DOC_KQ'),
                'SO_PHIM': self._get_int(cdha_elem, 'SO_PHIM'),
                'PHUONG_PHAP': self._get_text(cdha_elem, 'PHUONG_PHAP'),
                'MA_KHOA': self._get_text(cdha_elem, 'MA_KHOA'),
                'DU_PHONG': self._get_text(cdha_elem, 'DU_PHONG')
            }
            cdha_list.append(cdha)
        
        return cdha_list
    
    def _parse_giaiphaubenh(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        BẢNG BỔ SUNG: Giải phẫu bệnh
        """
        gpb_list = []
        
        for gpb_elem in root.findall('.//GIAI_PHAU_BENH') or root.findall('.//GPB'):
            gpb = {
                'MA_LK': self._get_text(gpb_elem, 'MA_LK'),
                'STT': self._get_int(gpb_elem, 'STT'),
                'MA_DICH_VU': self._get_text(gpb_elem, 'MA_DICH_VU'),
                'TEN_DICH_VU': self._get_text(gpb_elem, 'TEN_DICH_VU'),
                'NGAY_CHI_DINH': self._get_text(gpb_elem, 'NGAY_CHI_DINH'),
                'NGAY_KQ': self._get_text(gpb_elem, 'NGAY_KQ'),
                'MO_TA_DAI_THE': self._get_text(gpb_elem, 'MO_TA_DAI_THE'),
                'MO_TA_VI_THE': self._get_text(gpb_elem, 'MO_TA_VI_THE'),
                'KET_LUAN': self._get_text(gpb_elem, 'KET_LUAN'),
                'BS_CHI_DINH': self._get_text(gpb_elem, 'BS_CHI_DINH'),
                'BS_THUC_HIEN': self._get_text(gpb_elem, 'BS_THUC_HIEN'),
                'MA_BENH_GIAI_PHAU': self._get_text(gpb_elem, 'MA_BENH_GIAI_PHAU'),
                'TEN_BENH_GIAI_PHAU': self._get_text(gpb_elem, 'TEN_BENH_GIAI_PHAU'),
                'SO_BLOCK': self._get_int(gpb_elem, 'SO_BLOCK'),
                'SO_KINH': self._get_int(gpb_elem, 'SO_KINH'),
                'SO_MAU': self._get_int(gpb_elem, 'SO_MAU'),
                'DU_PHONG': self._get_text(gpb_elem, 'DU_PHONG')
            }
            gpb_list.append(gpb)
        
        return gpb_list
    
    def _parse_thuthuat_phauthuat(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        BẢNG BỔ SUNG: Thủ thuật, Phẫu thuật
        """
        pttt_list = []
        
        for pttt_elem in root.findall('.//THU_THUAT_PHAU_THUAT') or root.findall('.//PTTT'):
            pttt = {
                'MA_LK': self._get_text(pttt_elem, 'MA_LK'),
                'STT': self._get_int(pttt_elem, 'STT'),
                'MA_DICH_VU': self._get_text(pttt_elem, 'MA_DICH_VU'),
                'MA_PTTT': self._get_text(pttt_elem, 'MA_PTTT'),
                'TEN_PTTT': self._get_text(pttt_elem, 'TEN_PTTT'),
                'PHAN_LOAI_PTTT': self._get_int(pttt_elem, 'PHAN_LOAI_PTTT'),
                'NGAY_CHI_DINH': self._get_text(pttt_elem, 'NGAY_CHI_DINH'),
                'NGAY_THUC_HIEN': self._get_text(pttt_elem, 'NGAY_THUC_HIEN'),
                'NGUOI_CHI_DINH': self._get_text(pttt_elem, 'NGUOI_CHI_DINH'),
                'NGUOI_THUC_HIEN': self._get_text(pttt_elem, 'NGUOI_THUC_HIEN'),
                'PHUONG_PHAP_VO_CAM': self._get_text(pttt_elem, 'PHUONG_PHAP_VO_CAM'),
                'LOAI_PHAU_THUAT': self._get_int(pttt_elem, 'LOAI_PHAU_THUAT'),
                'PHUONG_PHAP_PHAU_THUAT': self._get_text(pttt_elem, 'PHUONG_PHAP_PHAU_THUAT'),
                'TAI_BIEN': self._get_text(pttt_elem, 'TAI_BIEN'),
                'BIEN_CHUNG': self._get_text(pttt_elem, 'BIEN_CHUNG'),
                'KET_QUA': self._get_text(pttt_elem, 'KET_QUA'),
                'MO_TA': self._get_text(pttt_elem, 'MO_TA'),
                'THOI_GIAN_PHAU_THUAT': self._get_float(pttt_elem, 'THOI_GIAN_PHAU_THUAT'),
                'LUONG_MAU_MAT': self._get_float(pttt_elem, 'LUONG_MAU_MAT'),
                'PHUONG_PHAP_DIEU_TRI': self._get_text(pttt_elem, 'PHUONG_PHAP_DIEU_TRI'),
                'MA_KHOA': self._get_text(pttt_elem, 'MA_KHOA'),
                'DU_PHONG': self._get_text(pttt_elem, 'DU_PHONG')
            }
            pttt_list.append(pttt)
        
        return pttt_list
    
    def _parse_giuong_benh(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        BẢNG BỔ SUNG: Chỉ định giường bệnh
        """
        giuong_list = []
        
        for giuong_elem in root.findall('.//GIUONG_BENH') or root.findall('.//GIUONG'):
            giuong = {
                'MA_LK': self._get_text(giuong_elem, 'MA_LK'),
                'STT': self._get_int(giuong_elem, 'STT'),
                'MA_GIUONG': self._get_text(giuong_elem, 'MA_GIUONG'),
                'MA_KHOA': self._get_text(giuong_elem, 'MA_KHOA'),
                'TEN_KHOA': self._get_text(giuong_elem, 'TEN_KHOA'),
                'MA_LOAI_GIUONG': self._get_text(giuong_elem, 'MA_LOAI_GIUONG'),
                'TEN_LOAI_GIUONG': self._get_text(giuong_elem, 'TEN_LOAI_GIUONG'),
                'TU_NGAY': self._get_text(giuong_elem, 'TU_NGAY'),
                'DEN_NGAY': self._get_text(giuong_elem, 'DEN_NGAY'),
                'SO_NGAY': self._get_int(giuong_elem, 'SO_NGAY'),
                'DON_GIA': self._get_float(giuong_elem, 'DON_GIA'),
                'THANH_TIEN': self._get_float(giuong_elem, 'THANH_TIEN'),
                'TYLE_TT_BH': self._get_float(giuong_elem, 'TYLE_TT_BH'),
                'T_BHTT': self._get_float(giuong_elem, 'T_BHTT'),
                'T_BNCCT': self._get_float(giuong_elem, 'T_BNCCT'),
                'T_BNTT': self._get_float(giuong_elem, 'T_BNTT'),
                'DU_PHONG': self._get_text(giuong_elem, 'DU_PHONG')
            }
            giuong_list.append(giuong)
        
        return giuong_list
    
    def _parse_truyen_mau(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        BẢNG BỔ SUNG: Truyền máu
        """
        mau_list = []
        
        for mau_elem in root.findall('.//TRUYEN_MAU') or root.findall('.//MAU'):
            mau = {
                'MA_LK': self._get_text(mau_elem, 'MA_LK'),
                'STT': self._get_int(mau_elem, 'STT'),
                'MA_DICH_VU': self._get_text(mau_elem, 'MA_DICH_VU'),
                'TEN_DICH_VU': self._get_text(mau_elem, 'TEN_DICH_VU'),
                'MA_THANH_PHAN_MAU': self._get_text(mau_elem, 'MA_THANH_PHAN_MAU'),
                'TEN_THANH_PHAN_MAU': self._get_text(mau_elem, 'TEN_THANH_PHAN_MAU'),
                'SO_LUONG': self._get_float(mau_elem, 'SO_LUONG'),
                'DON_VI_TINH': self._get_text(mau_elem, 'DON_VI_TINH'),
                'DON_GIA': self._get_float(mau_elem, 'DON_GIA'),
                'THANH_TIEN': self._get_float(mau_elem, 'THANH_TIEN'),
                'TYLE_TT_BH': self._get_float(mau_elem, 'TYLE_TT_BH'),
                'T_BHTT': self._get_float(mau_elem, 'T_BHTT'),
                'T_BNCCT': self._get_float(mau_elem, 'T_BNCCT'),
                'T_BNTT': self._get_float(mau_elem, 'T_BNTT'),
                'NGAY_CHI_DINH': self._get_text(mau_elem, 'NGAY_CHI_DINH'),
                'NGAY_THUC_HIEN': self._get_text(mau_elem, 'NGAY_THUC_HIEN'),
                'MA_KHOA': self._get_text(mau_elem, 'MA_KHOA'),
                'NGUOI_CHI_DINH': self._get_text(mau_elem, 'NGUOI_CHI_DINH'),
                'NGUOI_THUC_HIEN': self._get_text(mau_elem, 'NGUOI_THUC_HIEN'),
                'DU_PHONG': self._get_text(mau_elem, 'DU_PHONG')
            }
            mau_list.append(mau)
        
        return mau_list
    
    def _parse_vtyt_thaythe(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        BẢNG BỔ SUNG: Vật tư y tế thay thế (implant)
        """
        vtyt_list = []
        
        for vtyt_elem in root.findall('.//VTYT_THAY_THE') or root.findall('.//VTYT_TT'):
            vtyt = {
                'MA_LK': self._get_text(vtyt_elem, 'MA_LK'),
                'STT': self._get_int(vtyt_elem, 'STT'),
                'MA_VTYT': self._get_text(vtyt_elem, 'MA_VTYT'),
                'TEN_VTYT': self._get_text(vtyt_elem, 'TEN_VTYT'),
                'TEN_HANG_SX': self._get_text(vtyt_elem, 'TEN_HANG_SX'),
                'NUOC_SX': self._get_text(vtyt_elem, 'NUOC_SX'),
                'THONG_TIN_THAU': self._get_text(vtyt_elem, 'THONG_TIN_THAU'),
                'SO_LUONG': self._get_float(vtyt_elem, 'SO_LUONG'),
                'DON_VI_TINH': self._get_text(vtyt_elem, 'DON_VI_TINH'),
                'DON_GIA': self._get_float(vtyt_elem, 'DON_GIA'),
                'THANH_TIEN': self._get_float(vtyt_elem, 'THANH_TIEN'),
                'TYLE_TT_BH': self._get_float(vtyt_elem, 'TYLE_TT_BH'),
                'T_BHTT': self._get_float(vtyt_elem, 'T_BHTT'),
                'T_BNCCT': self._get_float(vtyt_elem, 'T_BNCCT'),
                'T_BNTT': self._get_float(vtyt_elem, 'T_BNTT'),
                'MA_KHOA': self._get_text(vtyt_elem, 'MA_KHOA'),
                'NGAY_CHI_DINH': self._get_text(vtyt_elem, 'NGAY_CHI_DINH'),
                'NGAY_THUC_HIEN': self._get_text(vtyt_elem, 'NGAY_THUC_HIEN'),
                'DU_PHONG': self._get_text(vtyt_elem, 'DU_PHONG')
            }
            vtyt_list.append(vtyt)
        
        return vtyt_list
    
    def _parse_dvkt_cao(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        BẢNG BỔ SUNG: Dịch vụ kỹ thuật cao
        """
        dvkt_cao_list = []
        
        for dvkt_elem in root.findall('.//DVKT_CAO') or root.findall('.//KT_CAO'):
            dvkt_cao = {
                'MA_LK': self._get_text(dvkt_elem, 'MA_LK'),
                'STT': self._get_int(dvkt_elem, 'STT'),
                'MA_DICH_VU': self._get_text(dvkt_elem, 'MA_DICH_VU'),
                'TEN_DICH_VU': self._get_text(dvkt_elem, 'TEN_DICH_VU'),
                'DON_VI_TINH': self._get_text(dvkt_elem, 'DON_VI_TINH'),
                'SO_LUONG': self._get_float(dvkt_elem, 'SO_LUONG'),
                'DON_GIA': self._get_float(dvkt_elem, 'DON_GIA'),
                'THANH_TIEN': self._get_float(dvkt_elem, 'THANH_TIEN'),
                'TYLE_TT_BH': self._get_float(dvkt_elem, 'TYLE_TT_BH'),
                'T_BHTT': self._get_float(dvkt_elem, 'T_BHTT'),
                'T_BNCCT': self._get_float(dvkt_elem, 'T_BNCCT'),
                'T_BNTT': self._get_float(dvkt_elem, 'T_BNTT'),
                'MA_KHOA': self._get_text(dvkt_elem, 'MA_KHOA'),
                'NGAY_CHI_DINH': self._get_text(dvkt_elem, 'NGAY_CHI_DINH'),
                'NGAY_THUC_HIEN': self._get_text(dvkt_elem, 'NGAY_THUC_HIEN'),
                'NGUOI_CHI_DINH': self._get_text(dvkt_elem, 'NGUOI_CHI_DINH'),
                'NGUOI_THUC_HIEN': self._get_text(dvkt_elem, 'NGUOI_THUC_HIEN'),
                'PHUONG_PHAP': self._get_text(dvkt_elem, 'PHUONG_PHAP'),
                'KET_QUA': self._get_text(dvkt_elem, 'KET_QUA'),
                'GHI_CHU': self._get_text(dvkt_elem, 'GHI_CHU'),
                'DU_PHONG': self._get_text(dvkt_elem, 'DU_PHONG')
            }
            dvkt_cao_list.append(dvkt_cao)
        
        return dvkt_cao_list
    
    # ============================================
    # Helper Methods
    # ============================================
    
    def _find_tonghop_element(self, root: ET.Element) -> ET.Element:
        """Tìm element tổng hợp"""
        elem = root.find('.//TONG_HOP')
        if elem is not None:
            return elem
        
        elem = root.find('.//BANG1')
        if elem is not None:
            return elem
        
        # Nếu không tìm thấy, return root
        return root
    
    def _get_text(self, element: ET.Element, path: str, default: str = '') -> str:
        """Get text value từ element"""
        if element is None:
            return default
        
        found = element.find(path)
        if found is not None and found.text:
            return found.text.strip()
        
        # Try as direct child
        found = element.find(path.split('//')[-1])
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
    
    def _count_records(self, root: ET.Element) -> Dict[str, int]:
        """Count số lượng records trong từng bảng"""
        return {
            'tonghop': 1,
            'thuoc': len(root.findall('.//CHI_TIET_THUOC')) or len(root.findall('.//THUOC')),
            'dvkt': len(root.findall('.//CHI_TIET_DVKT')) or len(root.findall('.//DVKT')),
            'cls': len(root.findall('.//CHI_TIET_CLS')) or len(root.findall('.//CAN_LAM_SANG')),
            'dbls': len(root.findall('.//DIEN_BIEN_LAM_SANG')) or len(root.findall('.//DBLS'))
        }
