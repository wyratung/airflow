"""
Complete BHYT Data Transformer - Parse đầy đủ TẤT CẢ 15 bảng XML
Theo Quyết định 4750/QĐ-BYT - Phiên bản 2.0
"""

import xml.etree.ElementTree as ET
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

class CompleteBHYTTransformer:
    """
    Transformer đầy đủ cho tất cả 15 bảng XML BHYT theo QĐ 4750
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.encoding = 'UTF-8'
    
    def transform_complete_xml(self, xml_content: str) -> Dict[str, Any]:
        """
        Parse đầy đủ TẤT CẢ 15 bảng XML BHYT theo QĐ 4750
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
        
        # Parse toàn bộ 15 bảng theo QĐ 4750
        dto = {
            'resourceType': 'BHYTClaimRecordComplete',
            'standard': 'QD_4750_BYT_v2.0',
            'version': '2.0',
            
            # HEADER - Thông tin file
            'header': self._parse_header(root),
            
            # XML1: Tổng hợp khám chữa bệnh
            'xml1_tonghop': self._parse_xml1_tonghop(root),
            
            # XML2: Chi tiết thuốc
            'xml2_chitiet_thuoc': self._parse_xml2_thuoc(root),
            
            # XML3: Chi tiết DVKT và VTYT
            'xml3_chitiet_dvkt': self._parse_xml3_dvkt(root),
            
            # XML4: Chi tiết dịch vụ cận lâm sàng
            'xml4_chitiet_cls': self._parse_xml4_cls(root),
            
            # XML5: Diễn biến lâm sàng
            'xml5_dienbienlamsang': self._parse_xml5_dienbienlamsang(root),
            
            # XML6: Chăm sóc điều trị HIV/AIDS
            'xml6_hivaids': self._parse_xml6_hivaids(root),
            
            # XML7: Chỉ định dược lưu
            'xml7_duoc_luu': self._parse_xml7_duocluu(root),
            
            # XML8: Chẩn đoán hình ảnh
            'xml8_chandoan_hinhanh': self._parse_xml8_cdha(root),
            
            # XML9: Thủ thuật phẫu thuật
            'xml9_phau_thuat': self._parse_xml9_phauthuat(root),
            
            # XML10: Truyền máu
            'xml10_truyen_mau': self._parse_xml10_truyenmau(root),
            
            # XML11: Chỉ số sinh tồn
            'xml11_chi_so_sinh_ton': self._parse_xml11_chiso_sinhton(root),
            
            # XML12: Thuốc điều trị ung thư
            'xml12_thuoc_ung_thu': self._parse_xml12_thuoc_ungthu(root),
            
            # XML13: Phục hồi chức năng
            'xml13_phuc_hoi_chuc_nang': self._parse_xml13_phuchoi(root),
            
            # XML14: Vật tư y tế thay thế
            'xml14_vtyt_thay_the': self._parse_xml14_vtyt_thaythe(root),
            
            # XML15: Giải phẫu bệnh
            'xml15_giai_phau_benh': self._parse_xml15_giaiphaubenh(root),
            
            # Metadata
            'metadata': {
                'transformedAt': datetime.utcnow().isoformat(),
                'transformer': 'CompleteBHYTTransformer_v2.0',
                'recordCount': self._count_all_records(root)
            }
        }
        
        self.logger.info(f"Complete transformation finished. Total records: {sum(dto['metadata']['recordCount'].values())}")
        return dto
    
    def _parse_header(self, root: ET.Element) -> Dict[str, Any]:
        """Parse header information từ GIAMDINHHS"""
        return {
            'MACSKCB': self._get_text(root, './/MACSKCB'),
            'NGAYLAP': self._get_text(root, './/NGAYLAP'),
            'SOLUONGHOSO': self._get_int(root, './/SOLUONGHOSO')
        }
    
    def _parse_xml1_tonghop(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        XML1: Chỉ tiêu tổng hợp khám chữa bệnh (ĐẦY ĐỦ THEO QĐ 4750)
        """
        tonghop_list = []
        for elem in root.findall('.//TONG_HOP') or [root]:
            if not self._get_text(elem, 'MA_LK'):
                continue
                
            tonghop = {
                # Mã liên kết
                'MA_LK': self._get_text(elem, 'MA_LK'),
                'STT': self._get_int(elem, 'STT'),
                
                # Thông tin bệnh nhân
                'MA_BN': self._get_text(elem, 'MA_BN'),
                'HO_TEN': self._get_text(elem, 'HO_TEN'),
                'SO_CCCD': self._get_text(elem, 'SO_CCCD'),
                'NGAY_SINH': self._get_text(elem, 'NGAY_SINH'),
                'GIOI_TINH': self._get_int(elem, 'GIOI_TINH'),
                'NHOM_MAU': self._get_text(elem, 'NHOM_MAU'),
                'MA_QUOCTICH': self._get_text(elem, 'MA_QUOCTICH'),
                'MA_DANTOC': self._get_text(elem, 'MA_DANTOC'),
                'MA_NGHE_NGHIEP': self._get_text(elem, 'MA_NGHE_NGHIEP'),
                
                # Địa chỉ
                'DIA_CHI': self._get_text(elem, 'DIA_CHI'),
                'MATINH_CU_TRU': self._get_text(elem, 'MATINH_CU_TRU'),
                'MAHUYEN_CU_TRU': self._get_text(elem, 'MAHUYEN_CU_TRU'),
                'MAXA_CU_TRU': self._get_text(elem, 'MAXA_CU_TRU'),
                'DIEN_THOAI': self._get_text(elem, 'DIEN_THOAI'),
                
                # Thẻ BHYT
                'MA_THE_BHYT': self._get_text(elem, 'MA_THE_BHYT'),
                'MA_DKBD': self._get_text(elem, 'MA_DKBD'),
                'GT_THE_TU': self._get_text(elem, 'GT_THE_TU'),
                'GT_THE_DEN': self._get_text(elem, 'GT_THE_DEN'),
                'NGAY_MIEN_CCT': self._get_text(elem, 'NGAY_MIEN_CCT'),
                
                # Thông tin khám chữa bệnh
                'LY_DO_VV': self._get_int(elem, 'LY_DO_VV'),
                'LY_DO_VNT': self._get_text(elem, 'LY_DO_VNT'),
                'MA_LY_DO_VNT': self._get_text(elem, 'MA_LY_DO_VNT'),
                'MA_NOI_DI': self._get_text(elem, 'MA_NOI_DI'),
                'MA_NOI_DEN': self._get_text(elem, 'MA_NOI_DEN'),
                'MA_TAI_NAN': self._get_int(elem, 'MA_TAI_NAN'),
                
                # Ngày giờ
                'NGAY_VAO': self._get_text(elem, 'NGAY_VAO'),
                'NGAY_RA': self._get_text(elem, 'NGAY_RA'),
                'SO_NGAY_DTRI': self._get_int(elem, 'SO_NGAY_DTRI'),
                
                # Kết quả điều trị
                'KET_QUA_DTRI': self._get_int(elem, 'KET_QUA_DTRI'),
                'TINH_TRANG_RV': self._get_int(elem, 'TINH_TRANG_RV'),
                
                # Thông tin cơ sở
                'MA_LOAI_KCB': self._get_int(elem, 'MA_LOAI_KCB'),
                'MA_KHOA': self._get_text(elem, 'MA_KHOA'),
                'MA_CSKCB': self._get_text(elem, 'MA_CSKCB'),
                'MA_KHUVUC': self._get_text(elem, 'MA_KHUVUC'),
                'MA_PTTT_QT': self._get_text(elem, 'MA_PTTT_QT'),
                
                # Thông tin bổ sung
                'CAN_NANG': self._get_float(elem, 'CAN_NANG'),
                'CAN_NANG_CON': self._get_float(elem, 'CAN_NANG_CON'),
                'NAM_NAM_LIEN_TUC': self._get_int(elem, 'NAM_NAM_LIEN_TUC'),
                'NGAY_TAI_KHAM': self._get_text(elem, 'NGAY_TAI_KHAM'),
                'MA_HSBA': self._get_text(elem, 'MA_HSBA'),
                'MA_TTDV': self._get_text(elem, 'MA_TTDV'),
                'DU_PHONG': self._get_text(elem, 'DU_PHONG')
            }
            tonghop_list.append(tonghop)
        
        return tonghop_list
    
    def _parse_xml2_thuoc(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        XML2: Chi tiết thuốc (THEO CHUẨN QĐ 4750)
        """
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
                'TYLE_TT_BH': self._get_int(elem, 'TYLE_TT_BH'),
                'SO_LUONG': self._get_float(elem, 'SO_LUONG'),
                'DON_GIA': self._get_float(elem, 'DON_GIA'),
                'THANH_TIEN_BV': self._get_float(elem, 'THANH_TIEN_BV'),
                'THANH_TIEN_BH': self._get_float(elem, 'THANH_TIEN_BH'),
                'T_NGUONKHAC_NSNN': self._get_float(elem, 'T_NGUONKHAC_NSNN'),
                'T_NGUONKHAC_VTNN': self._get_float(elem, 'T_NGUONKHAC_VTNN'),
                'T_NGUONKHAC_VTTN': self._get_float(elem, 'T_NGUONKHAC_VTTN'),
                'T_NGUONKHAC_CL': self._get_float(elem, 'T_NGUONKHAC_CL'),
                'T_NGUONKHAC': self._get_float(elem, 'T_NGUONKHAC'),
                'MUC_HUONG': self._get_int(elem, 'MUC_HUONG'),
                'T_BHTT': self._get_float(elem, 'T_BHTT'),
                'T_BNCCT': self._get_float(elem, 'T_BNCCT'),
                'T_BNTT': self._get_float(elem, 'T_BNTT'),
                'MA_KHOA': self._get_text(elem, 'MA_KHOA'),
                'MA_BAC_SI': self._get_text(elem, 'MA_BAC_SI'),
                'MA_DICH_VU': self._get_text(elem, 'MA_DICH_VU'),
                'NGAY_YL': self._get_text(elem, 'NGAY_YL'),
                'NGAY_TH_YL': self._get_text(elem, 'NGAY_TH_YL'),
                'MA_PTTT': self._get_int(elem, 'MA_PTTT'),
                'NGUON_CTRA': self._get_int(elem, 'NGUON_CTRA'),
                'VET_THUONG_TP': self._get_int(elem, 'VET_THUONG_TP'),
                'DU_PHONG': self._get_text(elem, 'DU_PHONG')
            }
            thuoc_list.append(thuoc)
        
        return thuoc_list
    
    def _parse_xml3_dvkt(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        XML3: Chi tiết DVKT và VTYT (THEO CHUẨN QĐ 4750)
        """
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
                'TT_THAU': self._get_text(elem, 'TT_THAU'),
                'TYLE_TT_DV': self._get_int(elem, 'TYLE_TT_DV'),
                'TYLE_TT_BH': self._get_int(elem, 'TYLE_TT_BH'),
                'THANH_TIEN_BV': self._get_float(elem, 'THANH_TIEN_BV'),
                'THANH_TIEN_BH': self._get_float(elem, 'THANH_TIEN_BH'),
                'T_TRANTT': self._get_float(elem, 'T_TRANTT'),
                'MUC_HUONG': self._get_int(elem, 'MUC_HUONG'),
                'T_NGUONKHAC_NSNN': self._get_float(elem, 'T_NGUONKHAC_NSNN'),
                'T_NGUONKHAC_VTNN': self._get_float(elem, 'T_NGUONKHAC_VTNN'),
                'T_NGUONKHAC_VTTN': self._get_float(elem, 'T_NGUONKHAC_VTTN'),
                'T_NGUONKHAC_CL': self._get_float(elem, 'T_NGUONKHAC_CL'),
                'T_NGUONKHAC': self._get_float(elem, 'T_NGUONKHAC'),
                'T_BHTT': self._get_float(elem, 'T_BHTT'),
                'T_BNTT': self._get_float(elem, 'T_BNTT'),
                'T_BNCCT': self._get_float(elem, 'T_BNCCT'),
                'MA_KHOA': self._get_text(elem, 'MA_KHOA'),
                'MA_GIUONG': self._get_text(elem, 'MA_GIUONG'),
                'MA_BAC_SI': self._get_text(elem, 'MA_BAC_SI'),
                'NGUOI_THUC_HIEN': self._get_text(elem, 'NGUOI_THUC_HIEN'),
                'MA_BENH': self._get_text(elem, 'MA_BENH'),
                'MA_BENH_YHCT': self._get_text(elem, 'MA_BENH_YHCT'),
                'NGAY_YL': self._get_text(elem, 'NGAY_YL'),
                'NGAY_TH_YL': self._get_text(elem, 'NGAY_TH_YL'),
                'NGAY_KQ': self._get_text(elem, 'NGAY_KQ'),
                'MA_PTTT': self._get_int(elem, 'MA_PTTT'),
                'VET_THUONG_TP': self._get_int(elem, 'VET_THUONG_TP'),
                'PP_VO_CAM': self._get_int(elem, 'PP_VO_CAM'),
                'VI_TRI_TH_DVKT': self._get_int(elem, 'VI_TRI_TH_DVKT'),
                'MA_MAY': self._get_text(elem, 'MA_MAY'),
                'MA_HIEU_SP': self._get_text(elem, 'MA_HIEU_SP'),
                'TAI_SU_DUNG': self._get_int(elem, 'TAI_SU_DUNG'),
                'DU_PHONG': self._get_text(elem, 'DU_PHONG')
            }
            dvkt_list.append(dvkt)
        
        return dvkt_list
    
    def _parse_xml4_cls(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        XML4: Chi tiết cận lâm sàng (THEO CHUẨN QĐ 4750)
        """
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
                'MA_BS_DOC_KQ': self._get_text(elem, 'MA_BS_DOC_KQ'),
                'DU_PHONG': self._get_text(elem, 'DU_PHONG')
            }
            cls_list.append(cls)
        
        return cls_list
    
    def _parse_xml5_dienbienlamsang(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        XML5: Diễn biến lâm sàng (THEO CHUẨN QĐ 4750)
        """
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
    
    def _parse_xml6_hivaids(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        XML6: Chăm sóc điều trị HIV/AIDS (MỚI THÊM)
        """
        hiv_list = []
        for elem in root.findall('.//CHI_TIET_HIVAIDS'):
            hiv = {
                'MA_LK': self._get_text(elem, 'MA_LK'),
                'STT': self._get_int(elem, 'STT'),
                'MA_BN_HIV': self._get_text(elem, 'MA_BN_HIV'),
                'LOAI_DIEU_TRI': self._get_int(elem, 'LOAI_DIEU_TRI'),
                'LAN_XET_NGHIEM': self._get_int(elem, 'LAN_XET_NGHIEM'),
                'NGAY_XET_NGHIEM': self._get_text(elem, 'NGAY_XET_NGHIEM'),
                'KET_QUA_XET_NGHIEM': self._get_text(elem, 'KET_QUA_XET_NGHIEM'),
                'MA_CSDT_HIV': self._get_text(elem, 'MA_CSDT_HIV'),
                'NGAY_BD_ARV': self._get_text(elem, 'NGAY_BD_ARV'),
                'PHAC_DO_DIEU_TRI': self._get_text(elem, 'PHAC_DO_DIEU_TRI'),
                'MA_BENH_HIV': self._get_text(elem, 'MA_BENH_HIV'),
                'TINH_TRANG_DTRI': self._get_int(elem, 'TINH_TRANG_DTRI'),
                'CD4': self._get_float(elem, 'CD4'),
                'VIRAL_LOAD': self._get_float(elem, 'VIRAL_LOAD'),
                'NGAY_TAI_KHAM': self._get_text(elem, 'NGAY_TAI_KHAM'),
                'DU_PHONG': self._get_text(elem, 'DU_PHONG')
            }
            hiv_list.append(hiv)
        
        return hiv_list
    
    def _parse_xml7_duocluu(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        XML7: Chỉ định dược lưu (MỚI THÊM)
        """
        duocluu_list = []
        for elem in root.findall('.//CHI_TIET_DUOC_LUU'):
            duocluu = {
                'MA_LK': self._get_text(elem, 'MA_LK'),
                'STT': self._get_int(elem, 'STT'),
                'SO_HIEU_BHXH': self._get_text(elem, 'SO_HIEU_BHXH'),
                'MA_THUOC': self._get_text(elem, 'MA_THUOC'),
                'TEN_THUOC': self._get_text(elem, 'TEN_THUOC'),
                'HAM_LUONG': self._get_text(elem, 'HAM_LUONG'),
                'SO_LUONG': self._get_float(elem, 'SO_LUONG'),
                'DON_VI_TINH': self._get_text(elem, 'DON_VI_TINH'),
                'DON_GIA': self._get_float(elem, 'DON_GIA'),
                'THANH_TIEN': self._get_float(elem, 'THANH_TIEN'),
                'MA_BAC_SI': self._get_text(elem, 'MA_BAC_SI'),
                'NGAY_CHI_DINH': self._get_text(elem, 'NGAY_CHI_DINH'),
                'NGAY_DIEU_TRI': self._get_text(elem, 'NGAY_DIEU_TRI'),
                'MA_BENH_CHINH': self._get_text(elem, 'MA_BENH_CHINH'),
                'SO_NGAY_DUOC_LUU': self._get_int(elem, 'SO_NGAY_DUOC_LUU'),
                'LY_DO_DUOC_LUU': self._get_text(elem, 'LY_DO_DUOC_LUU'),
                'DU_PHONG': self._get_text(elem, 'DU_PHONG')
            }
            duocluu_list.append(duocluu)
        
        return duocluu_list
    
    def _parse_xml8_cdha(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        XML8: Chẩn đoán hình ảnh (MỚI THÊM)
        """
        cdha_list = []
        for elem in root.findall('.//CHI_TIET_CDHA'):
            cdha = {
                'MA_LK': self._get_text(elem, 'MA_LK'),
                'STT': self._get_int(elem, 'STT'),
                'MA_DICH_VU': self._get_text(elem, 'MA_DICH_VU'),
                'TEN_DICH_VU': self._get_text(elem, 'TEN_DICH_VU'),
                'LOAI_CDHA': self._get_int(elem, 'LOAI_CDHA'),
                'VI_TRI_CHUP': self._get_text(elem, 'VI_TRI_CHUP'),
                'SO_PHIM': self._get_int(elem, 'SO_PHIM'),
                'MA_MAY_CDHA': self._get_text(elem, 'MA_MAY_CDHA'),
                'MO_TA_KQ': self._get_text(elem, 'MO_TA_KQ'),
                'KET_LUAN_CDHA': self._get_text(elem, 'KET_LUAN_CDHA'),
                'NGAY_CHUP': self._get_text(elem, 'NGAY_CHUP'),
                'NGAY_DOC_KQ': self._get_text(elem, 'NGAY_DOC_KQ'),
                'MA_BS_DOC_KQ': self._get_text(elem, 'MA_BS_DOC_KQ'),
                'MA_BS_CHI_DINH': self._get_text(elem, 'MA_BS_CHI_DINH'),
                'DUONG_LINK_HINH_ANH': self._get_text(elem, 'DUONG_LINK_HINH_ANH'),
                'DU_PHONG': self._get_text(elem, 'DU_PHONG')
            }
            cdha_list.append(cdha)
        
        return cdha_list
    
    def _parse_xml9_phauthuat(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        XML9: Thủ thuật phẫu thuật (MỚI THÊM)
        """
        pttt_list = []
        for elem in root.findall('.//CHI_TIET_PTTT'):
            pttt = {
                'MA_LK': self._get_text(elem, 'MA_LK'),
                'STT': self._get_int(elem, 'STT'),
                'MA_PTTT': self._get_text(elem, 'MA_PTTT'),
                'TEN_PTTT': self._get_text(elem, 'TEN_PTTT'),
                'LOAI_PTTT': self._get_int(elem, 'LOAI_PTTT'),
                'MA_BAC_SI_CHINH': self._get_text(elem, 'MA_BAC_SI_CHINH'),
                'MA_BAC_SI_PHU_1': self._get_text(elem, 'MA_BAC_SI_PHU_1'),
                'MA_BAC_SI_PHU_2': self._get_text(elem, 'MA_BAC_SI_PHU_2'),
                'MA_BAC_SI_GAY_ME': self._get_text(elem, 'MA_BAC_SI_GAY_ME'),
                'PHUONG_PHAP_VO_CAM': self._get_int(elem, 'PHUONG_PHAP_VO_CAM'),
                'VI_TRI_THUC_HIEN': self._get_int(elem, 'VI_TRI_THUC_HIEN'),
                'NGAY_BAT_DAU': self._get_text(elem, 'NGAY_BAT_DAU'),
                'NGAY_KET_THUC': self._get_text(elem, 'NGAY_KET_THUC'),
                'MA_BENH_TRUOC_PT': self._get_text(elem, 'MA_BENH_TRUOC_PT'),
                'MA_BENH_SAU_PT': self._get_text(elem, 'MA_BENH_SAU_PT'),
                'MO_TA_PTTT': self._get_text(elem, 'MO_TA_PTTT'),
                'KET_QUA_PTTT': self._get_text(elem, 'KET_QUA_PTTT'),
                'BIEN_CHUNG': self._get_text(elem, 'BIEN_CHUNG'),
                'LUONG_MAU_MAT': self._get_float(elem, 'LUONG_MAU_MAT'),
                'PHUONG_PHAP_PTTT': self._get_text(elem, 'PHUONG_PHAP_PTTT'),
                'DU_PHONG': self._get_text(elem, 'DU_PHONG')
            }
            pttt_list.append(pttt)
        
        return pttt_list
    
    def _parse_xml10_truyenmau(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        XML10: Truyền máu (MỚI THÊM)
        """
        mau_list = []
        for elem in root.findall('.//CHI_TIET_TRUYEN_MAU'):
            mau = {
                'MA_LK': self._get_text(elem, 'MA_LK'),
                'STT': self._get_int(elem, 'STT'),
                'MA_DICH_VU': self._get_text(elem, 'MA_DICH_VU'),
                'MA_THANH_PHAN_MAU': self._get_text(elem, 'MA_THANH_PHAN_MAU'),
                'TEN_THANH_PHAN_MAU': self._get_text(elem, 'TEN_THANH_PHAN_MAU'),
                'DON_VI_TINH': self._get_text(elem, 'DON_VI_TINH'),
                'SO_LUONG': self._get_float(elem, 'SO_LUONG'),
                'DON_GIA': self._get_float(elem, 'DON_GIA'),
                'THANH_TIEN': self._get_float(elem, 'THANH_TIEN'),
                'MA_NHOM_MAU': self._get_text(elem, 'MA_NHOM_MAU'),
                'SO_DANG_KY_TIEM': self._get_text(elem, 'SO_DANG_KY_TIEM'),
                'TT_THAU': self._get_text(elem, 'TT_THAU'),
                'NGAY_TRUYEN': self._get_text(elem, 'NGAY_TRUYEN'),
                'MA_BAC_SI_CHI_DINH': self._get_text(elem, 'MA_BAC_SI_CHI_DINH'),
                'MA_BAC_SI_THUC_HIEN': self._get_text(elem, 'MA_BAC_SI_THUC_HIEN'),
                'MA_BENH': self._get_text(elem, 'MA_BENH'),
                'LY_DO_TRUYEN': self._get_text(elem, 'LY_DO_TRUYEN'),
                'PHAN_UNG_SAU_TRUYEN': self._get_text(elem, 'PHAN_UNG_SAU_TRUYEN'),
                'DU_PHONG': self._get_text(elem, 'DU_PHONG')
            }
            mau_list.append(mau)
        
        return mau_list
    
    def _parse_xml11_chiso_sinhton(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        XML11: Chỉ số sinh tồn (MỚI THÊM)
        """
        csst_list = []
        for elem in root.findall('.//CHI_TIET_CHI_SO_SINH_TON'):
            csst = {
                'MA_LK': self._get_text(elem, 'MA_LK'),
                'STT': self._get_int(elem, 'STT'),
                'NGAY_DO': self._get_text(elem, 'NGAY_DO'),
                'NHIP_THO': self._get_int(elem, 'NHIP_THO'),
                'NHIP_TIM': self._get_int(elem, 'NHIP_TIM'),
                'HUYET_AP_MAX': self._get_int(elem, 'HUYET_AP_MAX'),
                'HUYET_AP_MIN': self._get_int(elem, 'HUYET_AP_MIN'),
                'NHIET_DO': self._get_float(elem, 'NHIET_DO'),
                'CAN_NANG': self._get_float(elem, 'CAN_NANG'),
                'CHIEU_CAO': self._get_float(elem, 'CHIEU_CAO'),
                'BMI': self._get_float(elem, 'BMI'),
                'SPO2': self._get_float(elem, 'SPO2'),
                'GLASGOW': self._get_int(elem, 'GLASGOW'),
                'NGUOI_THUC_HIEN': self._get_text(elem, 'NGUOI_THUC_HIEN'),
                'GHI_CHU': self._get_text(elem, 'GHI_CHU'),
                'DU_PHONG': self._get_text(elem, 'DU_PHONG')
            }
            csst_list.append(csst)
        
        return csst_list
    
    def _parse_xml12_thuoc_ungthu(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        XML12: Thuốc điều trị ung thư (MỚI THÊM)
        """
        ut_list = []
        for elem in root.findall('.//CHI_TIET_THUOC_UNG_THU'):
            ut = {
                'MA_LK': self._get_text(elem, 'MA_LK'),
                'STT': self._get_int(elem, 'STT'),
                'MA_THUOC': self._get_text(elem, 'MA_THUOC'),
                'TEN_THUOC': self._get_text(elem, 'TEN_THUOC'),
                'HAM_LUONG': self._get_text(elem, 'HAM_LUONG'),
                'DON_VI_TINH': self._get_text(elem, 'DON_VI_TINH'),
                'SO_LUONG': self._get_float(elem, 'SO_LUONG'),
                'DON_GIA': self._get_float(elem, 'DON_GIA'),
                'THANH_TIEN': self._get_float(elem, 'THANH_TIEN'),
                'MA_BENH_UNG_THU': self._get_text(elem, 'MA_BENH_UNG_THU'),
                'PHUONG_PHAP_DIEU_TRI': self._get_text(elem, 'PHUONG_PHAP_DIEU_TRI'),
                'DIEN_TICH_THAN_THE': self._get_float(elem, 'DIEN_TICH_THAN_THE'),
                'CAN_NANG': self._get_float(elem, 'CAN_NANG'),
                'SO_LAN_DIEU_TRI': self._get_int(elem, 'SO_LAN_DIEU_TRI'),
                'NGAY_DIEU_TRI': self._get_text(elem, 'NGAY_DIEU_TRI'),
                'MA_BAC_SI': self._get_text(elem, 'MA_BAC_SI'),
                'GHI_CHU': self._get_text(elem, 'GHI_CHU'),
                'DU_PHONG': self._get_text(elem, 'DU_PHONG')
            }
            ut_list.append(ut)
        
        return ut_list
    
    def _parse_xml13_phuchoi(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        XML13: Phục hồi chức năng (MỚI THÊM)
        """
        phcn_list = []
        for elem in root.findall('.//CHI_TIET_PHUC_HOI_CHUC_NANG'):
            phcn = {
                'MA_LK': self._get_text(elem, 'MA_LK'),
                'STT': self._get_int(elem, 'STT'),
                'MA_DICH_VU': self._get_text(elem, 'MA_DICH_VU'),
                'TEN_DICH_VU': self._get_text(elem, 'TEN_DICH_VU'),
                'LOAI_PHUC_HOI': self._get_int(elem, 'LOAI_PHUC_HOI'),
                'DON_VI_TINH': self._get_text(elem, 'DON_VI_TINH'),
                'SO_LUONG': self._get_float(elem, 'SO_LUONG'),
                'DON_GIA': self._get_float(elem, 'DON_GIA'),
                'THANH_TIEN': self._get_float(elem, 'THANH_TIEN'),
                'MA_BENH': self._get_text(elem, 'MA_BENH'),
                'TINH_TRANG_TRUOC': self._get_text(elem, 'TINH_TRANG_TRUOC'),
                'TINH_TRANG_SAU': self._get_text(elem, 'TINH_TRANG_SAU'),
                'NGAY_THUC_HIEN': self._get_text(elem, 'NGAY_THUC_HIEN'),
                'SO_BUOI_DIEU_TRI': self._get_int(elem, 'SO_BUOI_DIEU_TRI'),
                'MA_BAC_SI': self._get_text(elem, 'MA_BAC_SI'),
                'NGUOI_THUC_HIEN': self._get_text(elem, 'NGUOI_THUC_HIEN'),
                'KET_QUA_PHUC_HOI': self._get_text(elem, 'KET_QUA_PHUC_HOI'),
                'DU_PHONG': self._get_text(elem, 'DU_PHONG')
            }
            phcn_list.append(phcn)
        
        return phcn_list
    
    def _parse_xml14_vtyt_thaythe(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        XML14: Vật tư y tế thay thế (MỚI THÊM)
        """
        vtyt_list = []
        for elem in root.findall('.//CHI_TIET_VTYT_THAY_THE'):
            vtyt = {
                'MA_LK': self._get_text(elem, 'MA_LK'),
                'STT': self._get_int(elem, 'STT'),
                'MA_VTYT': self._get_text(elem, 'MA_VTYT'),
                'TEN_VTYT': self._get_text(elem, 'TEN_VTYT'),
                'LOAI_VTYT': self._get_int(elem, 'LOAI_VTYT'),
                'MA_NHOM_VTYT': self._get_text(elem, 'MA_NHOM_VTYT'),
                'DON_VI_TINH': self._get_text(elem, 'DON_VI_TINH'),
                'SO_LUONG': self._get_float(elem, 'SO_LUONG'),
                'DON_GIA': self._get_float(elem, 'DON_GIA'),
                'THANH_TIEN': self._get_float(elem, 'THANH_TIEN'),
                'TT_THAU': self._get_text(elem, 'TT_THAU'),
                'NUOC_SAN_XUAT': self._get_text(elem, 'NUOC_SAN_XUAT'),
                'NHA_SAN_XUAT': self._get_text(elem, 'NHA_SAN_XUAT'),
                'SO_DANG_KY': self._get_text(elem, 'SO_DANG_KY'),
                'NGAY_SAN_XUAT': self._get_text(elem, 'NGAY_SAN_XUAT'),
                'NGAY_HET_HAN': self._get_text(elem, 'NGAY_HET_HAN'),
                'NGAY_SU_DUNG': self._get_text(elem, 'NGAY_SU_DUNG'),
                'MA_BAC_SI': self._get_text(elem, 'MA_BAC_SI'),
                'VI_TRI_SU_DUNG': self._get_text(elem, 'VI_TRI_SU_DUNG'),
                'DU_PHONG': self._get_text(elem, 'DU_PHONG')
            }
            vtyt_list.append(vtyt)
        
        return vtyt_list
    
    def _parse_xml15_giaiphaubenh(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        XML15: Giải phẫu bệnh (MỚI THÊM)
        """
        gpb_list = []
        for elem in root.findall('.//CHI_TIET_GIAI_PHAU_BENH'):
            gpb = {
                'MA_LK': self._get_text(elem, 'MA_LK'),
                'STT': self._get_int(elem, 'STT'),
                'MA_DICH_VU': self._get_text(elem, 'MA_DICH_VU'),
                'TEN_DICH_VU': self._get_text(elem, 'TEN_DICH_VU'),
                'LOAI_MAU_BENH_PHAM': self._get_text(elem, 'LOAI_MAU_BENH_PHAM'),
                'MA_BENH_PHAM': self._get_text(elem, 'MA_BENH_PHAM'),
                'SO_LUONG_MAU': self._get_int(elem, 'SO_LUONG_MAU'),
                'NGAY_LAY_MAU': self._get_text(elem, 'NGAY_LAY_MAU'),
                'NGAY_NHAN_MAU': self._get_text(elem, 'NGAY_NHAN_MAU'),
                'VI_TRI_LAY_MAU': self._get_text(elem, 'VI_TRI_LAY_MAU'),
                'MO_TA_DAI_THE': self._get_text(elem, 'MO_TA_DAI_THE'),
                'MO_TA_VI_THE': self._get_text(elem, 'MO_TA_VI_THE'),
                'CHAN_DOAN_GPB': self._get_text(elem, 'CHAN_DOAN_GPB'),
                'MA_ICD_GPB': self._get_text(elem, 'MA_ICD_GPB'),
                'NGAY_KET_LUAN': self._get_text(elem, 'NGAY_KET_LUAN'),
                'MA_BAC_SI_CHI_DINH': self._get_text(elem, 'MA_BAC_SI_CHI_DINH'),
                'MA_BAC_SI_DOC_KQ': self._get_text(elem, 'MA_BAC_SI_DOC_KQ'),
                'DU_PHONG': self._get_text(elem, 'DU_PHONG')
            }
            gpb_list.append(gpb)
        
        return gpb_list
    
    # ============================================
    # Helper Methods
    # ============================================
    
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
    
    def _count_all_records(self, root: ET.Element) -> Dict[str, int]:
        """Count số lượng records trong TẤT CẢ 15 bảng"""
        return {
            'xml1_tonghop': len(root.findall('.//TONG_HOP')) or 1,
            'xml2_thuoc': len(root.findall('.//CHI_TIET_THUOC')),
            'xml3_dvkt': len(root.findall('.//CHI_TIET_DVKT')),
            'xml4_cls': len(root.findall('.//CHI_TIET_CLS')),
            'xml5_dienbienlamsang': len(root.findall('.//CHI_TIET_DIEN_BIEN_BENH')),
            'xml6_hivaids': len(root.findall('.//CHI_TIET_HIVAIDS')),
            'xml7_duocluu': len(root.findall('.//CHI_TIET_DUOC_LUU')),
            'xml8_cdha': len(root.findall('.//CHI_TIET_CDHA')),
            'xml9_phauthuat': len(root.findall('.//CHI_TIET_PTTT')),
            'xml10_truyenmau': len(root.findall('.//CHI_TIET_TRUYEN_MAU')),
            'xml11_chiso_sinhton': len(root.findall('.//CHI_TIET_CHI_SO_SINH_TON')),
            'xml12_thuoc_ungthu': len(root.findall('.//CHI_TIET_THUOC_UNG_THU')),
            'xml13_phuchoi': len(root.findall('.//CHI_TIET_PHUC_HOI_CHUC_NANG')),
            'xml14_vtyt_thaythe': len(root.findall('.//CHI_TIET_VTYT_THAY_THE')),
            'xml15_giaiphaubenh': len(root.findall('.//CHI_TIET_GIAI_PHAU_BENH'))
        }
