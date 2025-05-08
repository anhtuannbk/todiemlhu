import os
import pandas as pd
import logging
from glob import glob
from PyPDF2 import PdfReader, PdfWriter
from reportlab.lib import colors
from reportlab.pdfgen import canvas
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics
from reportlab.lib.pagesizes import letter
from io import BytesIO
import pdfplumber
import re
import concurrent.futures
import argparse
from pathlib import Path

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("todiem.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("todiem")

class GradeProcessor:
    def __init__(self, font_path=None):
        """Khởi tạo bộ xử lý điểm với đường dẫn font tùy chọn."""
        self.font_path = font_path or self._find_font_path()
        self._register_fonts()
        
    def _find_font_path(self):
        """Tìm đường dẫn font Arial trên hệ thống."""
        possible_paths = [
            "/content/todiem/arial.ttf",  # Đường dẫn cũ
            "./arial.ttf",                # Thư mục hiện tại
            "/usr/share/fonts/truetype/msttcorefonts/Arial.ttf",  # Linux
            "C:/Windows/Fonts/arial.ttf"  # Windows
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                return path
        
        logger.warning("Không tìm thấy font Arial. Sử dụng font mặc định.")
        return None
    
    def _register_fonts(self):
        """Đăng ký font để sử dụng trong PDF."""
        if self.font_path and os.path.exists(self.font_path):
            try:
                pdfmetrics.registerFont(TTFont('arial', self.font_path))
                logger.info(f"Đã đăng ký font từ {self.font_path}")
            except Exception as e:
                logger.error(f"Lỗi khi đăng ký font: {str(e)}")
                # Fallback to default font
                pdfmetrics.registerFont(TTFont('arial', pdfmetrics._findFontFile('Helvetica')))
        else:
            logger.warning("Sử dụng font mặc định thay vì Arial.")
        
    def load_excel_data(self, excel_path):
        """Đọc dữ liệu điểm từ file Excel."""
        try:
            if not os.path.exists(excel_path):
                logger.error(f"File Excel không tồn tại: {excel_path}")
                return None
                
            df = pd.read_excel(excel_path)
            
            # Kiểm tra các cột bắt buộc
            if 'Mã SV' not in df.columns or 'Điểm' not in df.columns:
                logger.error(f"File Excel thiếu cột 'Mã SV' hoặc 'Điểm': {excel_path}")
                return None
                
            # Chuẩn hóa dữ liệu
            df['Mã SV'] = df['Mã SV'].astype(str).str.strip()
            
            # Kiểm tra tính hợp lệ của điểm
            invalid_scores = df[(~df['Điểm'].isna()) & 
                               ((df['Điểm'] < 0) | (df['Điểm'] > 10))]['Mã SV'].tolist()
            if invalid_scores:
                logger.warning(f"Có {len(invalid_scores)} sinh viên có điểm không hợp lệ: {', '.join(invalid_scores[:5])}" + 
                             ("..." if len(invalid_scores) > 5 else ""))
            
            grades = dict(zip(df['Mã SV'], df['Điểm']))
            logger.info(f"Đã đọc {len(grades)} sinh viên từ file Excel {excel_path}")
            return grades
            
        except Exception as e:
            logger.error(f"Lỗi khi đọc file Excel {excel_path}: {str(e)}")
            return None

    def get_user_input_info(self, use_defaults=False):
        """Lấy thông tin cán bộ coi thi và chấm thi."""
        if use_defaults:
            return {
                "supervisor1": "Cán bộ coi thi 1",
                "supervisor2": "Cán bộ coi thi 2",
                "grader1": "Giảng viên chấm thi 1",
                "grader2": "Giảng viên chấm thi 2"
            }
            
        info = {}
        prompts = {
            "supervisor1": "Nhập tên cán bộ coi thi 1: ",
            "supervisor2": "Nhập tên cán bộ coi thi 2: ",
            "grader1": "Nhập tên giảng viên chấm thi 1: ",
            "grader2": "Nhập tên giảng viên chấm thi 2: "
        }
        
        for key, prompt in prompts.items():
            value = input(prompt).strip()
            info[key] = value if value else f"{key.capitalize()}"
            
        return info

    def convert_to_text(self, score):
        """Chuyển đổi điểm số thành chữ."""
        if pd.isna(score):
            return "Vắng"
            
        if not isinstance(score, (int, float)) or score < 0 or score > 10:
            return "Không hợp lệ"
            
        integer_part = int(score)
        decimal_part = score - integer_part
        
        number_words = ["Không", "Một", "Hai", "Ba", "Bốn", "Năm", "Sáu", "Bảy", "Tám", "Chín"]
        
        if integer_part == 10:
            return "Mười"
            
        if decimal_part == 0:
            return number_words[integer_part]
        elif round(decimal_part, 1) == 0.5:
            return f"{number_words[integer_part]} rưỡi"
        
        # Trường hợp khác (có thể mở rộng cho phần thập phân khác)
        return f"{number_words[integer_part]}"

    def find_grade_column(self, pdf_path):
        """Tìm vị trí cột điểm trong PDF."""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    words = page.extract_words()
                    for word in words:
                        if "Điểm" in word['text']:
                            return word['x0'], word['top'], word['bottom']
            logger.warning(f"Không tìm thấy cột 'Điểm' trong PDF {pdf_path}")
            return None
        except Exception as e:
            logger.error(f"Lỗi khi tìm cột điểm trong {pdf_path}: {str(e)}")
            return None

    def extract_student_positions(self, pdf_path):
        """Trích xuất vị trí của mã sinh viên trong PDF."""
        student_positions = {}
        student_id_pattern = r'\b[1-9]\d{8}\b'  # Mẫu cho mã sinh viên 9 chữ số, không bắt đầu bằng 0
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    words = page.extract_words()
                    for word in words:
                        text = word['text']
                        if re.match(student_id_pattern, text):
                            x0 = word['x0']
                            y0 = word['top']
                            student_positions[text] = (x0, y0, page_num)
                            
            logger.info(f"Đã tìm thấy {len(student_positions)} mã sinh viên trong PDF {pdf_path}")
            return student_positions
            
        except Exception as e:
            logger.error(f"Lỗi khi trích xuất vị trí sinh viên từ {pdf_path}: {str(e)}")
            return {}

    def add_grade_to_pdf(self, input_pdf, output_pdf, grades, info):
        """Thêm điểm vào PDF."""
        try:
            if not os.path.exists(input_pdf):
                logger.error(f"File PDF đầu vào không tồn tại: {input_pdf}")
                return False
                
            pdf_reader = PdfReader(input_pdf)
            pdf_writer = PdfWriter()
            
            # Tìm vị trí cột điểm
            grade_column = self.find_grade_column(input_pdf)
            if grade_column is None:
                logger.error(f"Không tìm thấy cột 'Điểm' trong PDF {input_pdf}")
                return False
                
            column_x, column_top, column_bottom = grade_column
            
            # Lấy vị trí của sinh viên
            student_positions = self.extract_student_positions(input_pdf)
            if not student_positions:
                logger.error(f"Không tìm thấy mã sinh viên nào trong PDF {input_pdf}")
                return False
                
            # Tính số sinh viên vắng
            total_students = len(student_positions)
            absent_count = sum(1 for ma_sv in student_positions if ma_sv in grades and pd.isna(grades[ma_sv]))
            logger.info(f"Tổng số: {total_students} sinh viên, vắng: {absent_count} sinh viên")
            
            # Đếm số sinh viên không có trong Excel
            missing_students = sum(1 for ma_sv in student_positions if ma_sv not in grades)
            if missing_students > 0:
                logger.warning(f"Có {missing_students} mã SV trong PDF không có trong file Excel")
            
            # Xử lý từng trang PDF
            for page_num in range(len(pdf_reader.pages)):
                page = pdf_reader.pages[page_num]
                packet = BytesIO()
                can = canvas.Canvas(packet, pagesize=letter)
                can.setFont("arial", 9)
                
                # Thêm thông tin tổng số sinh viên và cán bộ vào trang đầu
                if page_num == 0:
                    self._add_header_info(can, total_students, absent_count, info)
                
                # Thêm điểm cho sinh viên trong trang này
                self._add_student_grades(can, student_positions, grades, page_num, column_x)
                
                can.save()
                packet.seek(0)
                
                try:
                    new_pdf_reader = PdfReader(BytesIO(packet.read()))
                    if len(new_pdf_reader.pages) > 0:
                        page.merge_page(new_pdf_reader.pages[0])
                except Exception as e:
                    logger.error(f"Lỗi khi tạo trang mới {page_num+1}: {str(e)}")
                
                pdf_writer.add_page(page)
            
            # Lưu file kết quả
            output_dir = os.path.dirname(output_pdf)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
                
            with open(output_pdf, "wb") as output_file:
                pdf_writer.write(output_file)
                
            logger.info(f"Đã tạo file {output_pdf} thành công")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi thêm điểm vào PDF {input_pdf}: {str(e)}")
            return False
    
    def _add_header_info(self, canvas, total_students, absent_count, info):
        """Thêm thông tin header vào trang đầu tiên."""
        try:
            canvas.drawString(125, 93.5, f"{total_students}")
            canvas.drawString(125, 75, f"{absent_count}")
            canvas.drawCentredString(380, 52, f"{info['supervisor1']}")
            canvas.drawCentredString(380, 15, f"{info['supervisor2']}")
            canvas.drawCentredString(505, 52, f"{info['grader1']}")
            canvas.drawCentredString(505, 15, f"{info['grader2']}")
        except Exception as e:
            logger.error(f"Lỗi khi thêm thông tin header: {str(e)}")
    
    def _add_student_grades(self, canvas, student_positions, grades, page_num, column_x):
        """Thêm điểm cho sinh viên trong trang hiện tại."""
        for ma_sv_pdf, position in student_positions.items():
            if position[2] - 1 != page_num:
                continue
                
            ma_sv_pdf = str(ma_sv_pdf).strip()
            x = column_x - 12
            y = position[1]
            y_adjusted = 832 - y
            
            if ma_sv_pdf in grades:
                score = grades[ma_sv_pdf]
                grade_text = self.convert_to_text(score)
                canvas.drawString(x, y_adjusted, grade_text)
                
                if not pd.isna(score) and 0 <= score <= 10:
                    self._draw_score_circles(canvas, column_x, y_adjusted, score)
            else:
                logger.debug(f"Mã SV {ma_sv_pdf} không có trong file Excel")
    
    def _draw_score_circles(self, canvas, column_x, y_adjusted, score):
        """Vẽ các vòng tròn đánh dấu điểm."""
        try:
            # Vẽ vòng tròn cho phần nguyên
            circle_x = column_x + 44.5 + int(score) * 16.7
            circle_y = y_adjusted + 3
            circle_diameter = 4 * 2.8346
            radius = circle_diameter / 2
            canvas.setFillColor(colors.black)
            canvas.circle(circle_x, circle_y, radius, fill=1)
            
            # Vẽ vòng tròn cho phần thập phân 0.5 nếu có
            if round(score - int(score), 1) == 0.5:
                additional_circle_x = column_x + 229
                additional_circle_y = y_adjusted + 3
                canvas.circle(additional_circle_x, additional_circle_y, radius, fill=1)
        except Exception as e:
            logger.error(f"Lỗi khi vẽ vòng tròn điểm: {str(e)}")

    def prepare_grade_files(self, input_excel):
        """Chuẩn bị các file điểm từ file Excel đầu vào."""
        try:
            if not os.path.exists(input_excel):
                logger.error(f"File Excel đầu vào không tồn tại: {input_excel}")
                return False
                
            df = pd.read_excel(input_excel)
            
            # Kiểm tra các cột
            required_columns = ['StudentID']
            grade_columns = {
                'qt': 'Điểm quá trình',
                'gk': 'Điểm giữa kỳ',
                'ck': 'Điểm cuối kỳ'
            }
            
            if 'StudentID' not in df.columns:
                logger.error(f"File Excel thiếu cột StudentID: {input_excel}")
                return False
            
            # Kiểm tra các cột điểm
            available_types = []
            for grade_type, column in grade_columns.items():
                if column in df.columns:
                    grade_df = df[['StudentID', column]].copy()
                    grade_df.columns = ['Mã SV', 'Điểm']
                    grade_file = f'grade_{grade_type}.xlsx'
                    grade_df.to_excel(grade_file, index=False)
                    logger.info(f"Đã tạo file {grade_file} thành công")
                    available_types.append(grade_type)
            
            if not available_types:
                logger.error(f"Không tìm thấy cột điểm nào trong file Excel: {input_excel}")
                return False
                
            return available_types
            
        except Exception as e:
            logger.error(f"Lỗi khi chuẩn bị file điểm từ {input_excel}: {str(e)}")
            return False

    def rename_pdf_files(self, directory):
        """Đổi tên các file PDF dựa trên nội dung."""
        renamed_files = {}
        keywords = {
            'quá trình': '_qt',
            'giữa kỳ': '_gk', 
            'cuối kỳ': '_ck'
        }
        
        for file_name in os.listdir(directory):
            if not file_name.endswith(".pdf"):
                continue
                
            file_path = os.path.join(directory, file_name)
            
            try:
                with open(file_path, "rb") as pdf_file:
                    reader = PdfReader(pdf_file)
                    content = ""
                    for page in reader.pages:
                        content += page.extract_text() or ""
                    
                    suffix = None
                    for keyword, suffix_value in keywords.items():
                        if keyword in content.lower():
                            suffix = suffix_value
                            break
                    
                    if suffix:
                        base_name = file_name.rsplit('.', 1)[0]
                        if not base_name.endswith(tuple(keywords.values())):
                            new_file_name = f"{base_name}{suffix}.pdf"
                            new_file_path = os.path.join(directory, new_file_name)
                            
                            # Nếu file đích đã tồn tại, thêm số thứ tự
                            counter = 1
                            while os.path.exists(new_file_path):
                                new_file_name = f"{base_name}{suffix}_{counter}.pdf"
                                new_file_path = os.path.join(directory, new_file_name)
                                counter += 1
                                
                            os.rename(file_path, new_file_path)
                            logger.info(f"Đổi tên {file_name} thành {new_file_name}")
                            renamed_files[file_name] = new_file_name
            except Exception as e:
                logger.error(f"Lỗi khi đổi tên file {file_name}: {str(e)}")
        
        return renamed_files

    def process_grade_type(self, directory, grade_type, info):
        """Xử lý một loại điểm cụ thể."""
        excel_file = os.path.join(directory, f'grade_{grade_type}.xlsx')
        if not os.path.exists(excel_file):
            logger.warning(f"Không tìm thấy file Excel {excel_file}")
            return 0
            
        pdf_files = [f for f in os.listdir(directory) 
                    if f.endswith('.pdf') and f'{grade_type}.pdf' in f]
                    
        if not pdf_files:
            logger.warning(f"Không tìm thấy file PDF nào cho loại điểm {grade_type}")
            return 0
            
        grades = self.load_excel_data(excel_file)
        if grades is None:
            return 0
            
        success_count = 0
        for pdf_file in pdf_files:
            pdf_path = os.path.join(directory, pdf_file)
            output_pdf = os.path.join(directory, f'output_{pdf_file}')
            
            if self.add_grade_to_pdf(pdf_path, output_pdf, grades, info):
                success_count += 1
                
        return success_count

    def process_files(self, directory, info, parallel=True):
        """Xử lý tất cả các file trong thư mục."""
        # Chuẩn bị file điểm
        xlsx_files = glob(os.path.join(directory, "*.xlsx"))
        if not xlsx_files:
            logger.error("Không tìm thấy file .xlsx nào.")
            return False
            
        # Sử dụng file Excel đầu tiên tìm thấy
        input_file = xlsx_files[0]
        available_types = self.prepare_grade_files(input_file)
        
        if not available_types:
            return False
            
        # Đổi tên file PDF theo loại điểm
        self.rename_pdf_files(directory)
        
        # Xử lý từng loại điểm
        if parallel and len(available_types) > 1:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {
                    executor.submit(self.process_grade_type, directory, grade_type, info): grade_type 
                    for grade_type in available_types
                }
                for future in concurrent.futures.as_completed(futures):
                    grade_type = futures[future]
                    try:
                        count = future.result()
                        logger.info(f"Đã xử lý {count} file cho loại điểm {grade_type}")
                    except Exception as e:
                        logger.error(f"Lỗi khi xử lý loại điểm {grade_type}: {str(e)}")
        else:
            for grade_type in available_types:
                count = self.process_grade_type(directory, grade_type, info)
                logger.info(f"Đã xử lý {count} file cho loại điểm {grade_type}")
        
        return True

    def cleanup_files(self, directory, keep_originals=False):
        """Dọn dẹp các file tạm sau khi xử lý."""
        try:
            if not keep_originals:
                # Xóa file PDF gốc (không có 'output' trong tên)
                for file in os.listdir(directory):
                    file_path = os.path.join(directory, file)
                    if file.endswith('.pdf') and 'output' not in file.lower():
                        os.remove(file_path)
                        logger.debug(f"Đã xóa file {file}")
            
            # Xóa file Excel tạm
            for file in os.listdir(directory):
                file_path = os.path.join(directory, file)
                if file.startswith('grade_') and file.endswith('.xlsx'):
                    os.remove(file_path)
                    logger.debug(f"Đã xóa file {file}")
                    
            logger.info("Đã dọn dẹp các file tạm")
            return True
        except Exception as e:
            logger.error(f"Lỗi khi dọn dẹp file: {str(e)}")
            return False

def main():
    parser = argparse.ArgumentParser(description="Phần mềm tô điểm tự động")
    parser.add_argument("--dir", help="Thư mục chứa file (mặc định: thư mục hiện tại)", 
                        default=os.getcwd())
    parser.add_argument("--font", help="Đường dẫn đến font Arial", 
                        default=None)
    parser.add_argument("--keep", help="Giữ lại file gốc", 
                        action="store_true")
    parser.add_argument("--default-info", help="Sử dụng thông tin mặc định", 
                        action="store_true")
    parser.add_argument("--verbose", help="Hiển thị nhiều thông tin hơn", 
                        action="store_true")
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    work_dir = os.path.abspath(args.dir)
    logger.info(f"Bắt đầu xử lý trong thư mục: {work_dir}")
    
    try:
        processor = GradeProcessor(font_path=args.font)
        info = processor.get_user_input_info(use_defaults=args.default_info)
        
        if processor.process_files(work_dir, info):
            processor.cleanup_files(work_dir, keep_originals=args.keep)
            logger.info("Hoàn thành! Kiểm tra các file output_*.pdf trong thư mục.")
        else:
            logger.error("Có lỗi xảy ra trong quá trình xử lý.")
            
    except KeyboardInterrupt:
        logger.info("Đã hủy bởi người dùng.")
    except Exception as e:
        logger.error(f"Lỗi không xác định: {str(e)}")

if __name__ == "__main__":
    main()
