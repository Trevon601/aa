import pandas as pd  
import time  
import random  
import re  
import os  
import sys  
import json  
import unidecode  
import logging  
from collections import defaultdict  
from concurrent.futures import ThreadPoolExecutor  

from selenium import webdriver  
from selenium.webdriver.chrome.options import Options  
from selenium.webdriver.chrome.service import Service  
from selenium.webdriver.common.by import By  
from selenium.webdriver.support.ui import WebDriverWait  
from selenium.webdriver.support import expected_conditions as EC



def setup_logger(name):  
    """Tạo và cấu hình logger với tên cụ thể"""  
    logger = logging.getLogger(name)  
    logger.setLevel(logging.INFO)  

    # Xóa handlers hiện có để tránh trùng lặp log  
    if logger.hasHandlers():  
        logger.handlers.clear()  

    # Tạo formatter với tên logger  
    formatter = logging.Formatter(f'%(asctime)s - {name} - %(levelname)s - %(message)s')  

    # Thêm console handler  
    console_handler = logging.StreamHandler(sys.stdout)  
    console_handler.setFormatter(formatter)  
    logger.addHandler(console_handler)  

    # Thêm file handler để lưu log ra file  
    try:  
        file_handler = logging.FileHandler(f"{name}.log", encoding='utf-8')  
        file_handler.setFormatter(formatter)  
        logger.addHandler(file_handler)  
    except Exception as e:  
        print(f"Không thể tạo file log: {e}")  

    return logger



class AmazonScraper:  
    """Lớp chịu trách nhiệm scrape thông tin sản phẩm từ Amazon"""  
    
    def __init__(self, driver=None, logger=None):  
        """Khởi tạo scraper với WebDriver"""  
        if driver is None:  
            self.driver = self._create_driver()  
            self.should_quit_driver = True  
        else:  
            self.driver = driver  
            self.should_quit_driver = False  
            
        self.logger = logger or logging.getLogger(__name__)  
        
        # Thêm User-Agent ngẫu nhiên  
        self.user_agents = [  
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',  
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',  
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15',  
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0'  
        ]  
    
    def __del__(self):  
        """Hủy scraper, đóng driver nếu do scraper tạo"""  
        if hasattr(self, 'driver') and self.should_quit_driver:  
            try:  
                self.driver.quit()  
            except:  
                pass  
    
    def _create_driver(self):  
        """Tạo và trả về WebDriver Selenium"""  
        chrome_options = Options()  
        chrome_options.add_argument("--headless")  
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')  
        
        # Thêm user agent ngẫu nhiên  
        user_agent = random.choice(self.user_agents)  
        chrome_options.add_argument(f'user-agent={user_agent}')  
        
        # Thêm các tùy chọn khác để tránh phát hiện  
        chrome_options.add_argument("--disable-notifications")  
        chrome_options.add_argument("--disable-popup-blocking")  
        chrome_options.add_argument("--disable-extensions")  
        chrome_options.add_argument("--disable-dev-shm-usage")  
        chrome_options.add_argument("--no-sandbox")  
        
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])  
        chrome_options.add_experimental_option('useAutomationExtension', False)  
        
        driver = webdriver.Chrome(options=chrome_options)  
        
        # Thêm JavaScript để tránh phát hiện Selenium  
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")  
        
        return driver  
    
    @staticmethod  
    def slugify(text):  
        """Tạo slug từ text"""  
        text = unidecode.unidecode(text)  
        text = re.sub(r"[^\w\s-]", '', text).strip().lower()  
        return re.sub(r"[-\s]+", '-', text)  
    
    @staticmethod  
    def convert_to_fullsize(img_url):  
        """Chuyển URL hình ảnh sang kích thước đầy đủ"""  
        if not img_url:  
            return img_url  
        return img_url.replace('_SS40_', '') \
                      .replace('_SX40_', '') \
                      .replace('_SY40_', '') \
                      .replace('_AC_US40_', '') \
                      .replace('_AC_US100_', '') \
                      .replace('_AC_SY400_', '') \
                      .replace('_AC_SY879_', '') \
                      .replace('_AC_SR38,50_', '') \
                      .replace('_CR40,40,400,400_', '')  
    
    def load_product_page(self, asin):  
        """Tải trang sản phẩm và chờ các phần tử cần thiết"""  
        url = f'https://www.amazon.com/dp/{asin}'  
        
        # Thêm thời gian chờ ngẫu nhiên trước khi request để tránh bị phát hiện  
        time.sleep(random.uniform(1, 3))  
        
        # Mở trang sản phẩm  
        self.driver.get(url)  
        
        # Chờ trang tải và kiểm tra phần tử quan trọng  
        try:  
            wait = WebDriverWait(self.driver, 20)  
            
            # Chờ phần tử tiêu đề xuất hiện  
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "span#productTitle")))  
            
            # Thời gian chờ tĩnh để JavaScript tải xong  
            time.sleep(random.uniform(3, 5))  
            
            # Cuộn trang để tải tất cả nội dung  
            self._scroll_page()  
            
            return True  
        except Exception as e:  
            self.logger.warning(f"Timeout loading page for ASIN {asin}: {e}")  
            
            # Kiểm tra xem có trang CAPTCHA không  
            try:  
                if "captcha" in self.driver.title.lower() or "robot" in self.driver.page_source.lower():  
                    self.logger.error(f"Amazon đang yêu cầu CAPTCHA. IP của bạn có thể đã bị hạn chế tạm thời.")  
            except:  
                pass  
                
            return False  
    
    def _scroll_page(self):  
        """Cuộn trang để tải tất cả nội dung JavaScript"""  
        try:  
            # Chiều cao của trang  
            last_height = self.driver.execute_script("return document.body.scrollHeight")  
            
            # Cuộn từ từ xuống dưới  
            for i in range(3):  # Cuộn 3 lần  
                # Cuộn xuống một phần  
                scroll_point = (i + 1) * last_height // 3  
                self.driver.execute_script(f"window.scrollTo(0, {scroll_point});")  
                time.sleep(random.uniform(0.5, 1))  
            
            # Cuộn lại lên trên  
            self.driver.execute_script("window.scrollTo(0, 0);")  
            time.sleep(random.uniform(0.5, 1))  
        except Exception as e:  
            self.logger.warning(f"Không thể cuộn trang: {e}")  
    
    def get_title(self, asin):  
        """Lấy tiêu đề sản phẩm"""  
        try:  
            # Tìm tiêu đề bằng selector chính xác  
            title = self.driver.find_element(By.CSS_SELECTOR, "span#productTitle").text.strip()  
            if title:  
                self.logger.info(f"Đã lấy tiêu đề: {title[:50]}...")  
                return title  
        except Exception as e:  
            self.logger.warning(f"Không lấy được title ASIN {asin} bằng selector chính: {e}")  
            
            # Thử với selector dự phòng  
            try:  
                title = self.driver.find_element(By.ID, "productTitle").text.strip()  
                if title:  
                    return title  
            except:  
                pass  
        
        self.logger.warning(f"Không tìm thấy tiêu đề cho ASIN {asin}")  
        return ""  
    
    def get_description(self, asin):  
        """Lấy mô tả ngắn (bullet points)"""  
        try:  
            # Sử dụng selector đúng cho phần mô tả ngắn  
            desc_element = self.driver.find_element(By.CSS_SELECTOR, "div#feature-bullets")  
            
            # Lấy tất cả các điểm bullet  
            bullet_points = desc_element.find_elements(By.CSS_SELECTOR, "li span.a-list-item")  
            
            # Gộp các điểm bullet thành một chuỗi HTML  
            if bullet_points:  
                desc = ""  
                for point in bullet_points:  
                    text = point.text.strip()  
                    if text:  
                        desc += f"• {text}<br>"  
                return desc  
            else:  
                # Nếu không tìm thấy điểm bullet, lấy toàn bộ text  
                desc = desc_element.text.strip().replace('\n', '<br>')  
                return desc  
        except Exception as e:  
            self.logger.warning(f"Không lấy được mô tả ngắn của ASIN {asin}: {e}")  
            return ""  
    
    def get_detailed_description(self, asin):  
        """Lấy mô tả chi tiết"""  
        try:  
            # Thử lấy từ product description  
            detailed_desc = self.driver.find_element(By.ID, "productDescription").text.strip()  
            if detailed_desc:  
                return detailed_desc.replace('\n', '<br>')  
        except:  
            pass  
            
        # Thử các selector khác cho mô tả chi tiết  
        selectors = [  
            "#aplus",  
            "#dpx-aplus-product-description_feature_div",  
            "#aplus3p_feature_div",  
            "#descriptionAndDetails"  
        ]  
        
        for selector in selectors:  
            try:  
                element = self.driver.find_element(By.CSS_SELECTOR, selector)  
                detailed_desc = element.text.strip()  
                if detailed_desc:  
                    return detailed_desc.replace('\n', '<br>')  
            except:  
                continue  
        
        self.logger.warning(f"Không lấy được mô tả chi tiết của ASIN {asin}")  
        return ""  
    
    def get_price(self, asin):  
        """Lấy giá sản phẩm, sử dụng selector chính xác"""  
        price_selectors = [  
            # Selector bạn đã tìm thấy  
            "#tp-tool-tip-subtotal-price-value",  
            
            # Các selector phổ biến khác  
            "span.a-price > span.a-offscreen",  
            "#priceblock_ourprice",  
            "#priceblock_dealprice",  
            ".apexPriceToPay > span[aria-hidden='true']",  
            ".a-price .a-offscreen",  
            "#corePrice_feature_div .a-price .a-offscreen",  
            "#corePrice_desktop .a-price .a-offscreen"  
        ]  
        
        # Chờ thêm để đảm bảo giá được cập nhật  
        time.sleep(1)  
        
        for sel in price_selectors:  
            try:  
                price_elem = self.driver.find_element(By.CSS_SELECTOR, sel)  
                price_raw = price_elem.text or price_elem.get_attribute('innerHTML')  
                
                if price_raw:  
                    # Xử lý giá, loại bỏ ký tự $ và dấu phẩy  
                    price = re.sub(r'[^\d.]', '', price_raw)  
                    if price:  
                        self.logger.info(f"Đã lấy giá {price} bằng selector {sel}")  
                        return price  
            except:  
                continue  
        
        self.logger.warning(f"Không lấy được giá của ASIN {asin}")  
        return ""  
    
    def get_brand(self, asin):  
        """Lấy thương hiệu sản phẩm"""  
        selectors = [  
            'a#bylineInfo',  
            '#bylineInfo',  
            '.a-row.a-spacing-small .a-link-normal[href*="brandtextbin"]',  
            'a#brand',  
            '#brand',  
            'a[id*="brand"]',  
            'tr.a-spacing-small:has(th:contains("Brand")) td'  
        ]  
        for sel in selectors:  
            try:  
                brand = self.driver.find_element(By.CSS_SELECTOR, sel).text.strip()  
                if brand:  
                    # Làm sạch text, chỉ lấy tên thương hiệu  
                    brand = re.sub(r'^(Brand:|Visit the|Visit) ', '', brand, flags=re.IGNORECASE)  
                    brand = re.sub(r' (Store|Brand|Page)$', '', brand, flags=re.IGNORECASE)  
                    self.logger.info(f"Đã lấy thương hiệu: {brand}")  
                    return brand.strip()  
            except:  
                continue  
        return ""  
    
    def get_images(self, asin, max_images=5):  
        """Lấy hình ảnh sản phẩm"""  
        img_urls = []  
        
        # Thêm thời gian chờ để JavaScript tải ảnh  
        time.sleep(2)  

        # Lấy ảnh chính  
        try:  
            # Tìm ảnh chính  
            main_img = self.driver.find_element(By.ID, "landingImage")  
            
            # Thử lấy giá trị từ các thuộc tính khác nhau  
            for attr in ['data-old-hires', 'data-zoom-hires', 'src']:  
                src = main_img.get_attribute(attr)  
                if src:  
                    img_urls.append(self.convert_to_fullsize(src))  
                    self.logger.info(f"Đã lấy ảnh chính từ thuộc tính {attr}")  
                    break  
        except Exception as e:  
            self.logger.warning(f"Không tìm thấy ảnh chính của ASIN {asin}: {e}")  

        # Lấy các thumbnail  
        thumb_set = set(img_urls)  # Để tránh trùng lặp  
        
        # Thử nhiều cách khác nhau để lấy thumbnail  
        thumbnail_selectors = [  
            '#altImages li img',  
            'li.a-spacing-small.item img',  
            'li.image.item img',  
            '#altImages .a-button-thumbnail img'  
        ]  
        
        for selector in thumbnail_selectors:  
            if len(img_urls) >= max_images:  
                break  
                
            try:  
                thumbnails = self.driver.find_elements(By.CSS_SELECTOR, selector)  
                
                for thumb in thumbnails:  
                    if len(img_urls) >= max_images:  
                        break  
                        
                    # Thử nhiều thuộc tính khác nhau  
                    for attr in ['data-old-hires', 'data-large-image', 'src']:  
                        thumb_src = thumb.get_attribute(attr)  
                        if not thumb_src:  
                            continue  
                            
                        # Lọc các ảnh không phải sản phẩm  
                        if re.search(r'_CB\d+.*_FMpng_RI_', thumb_src) or "sprite" in thumb_src:  
                            continue  
                            
                        full_img = self.convert_to_fullsize(thumb_src)  
                        if full_img and full_img not in thumb_set:  
                            img_urls.append(full_img)  
                            thumb_set.add(full_img)  
                            break  
                
                # Nếu đã tìm thấy ít nhất một thumbnail, dừng lại  
                if len(img_urls) > 1:  
                    break  
            except Exception as e:  
                self.logger.debug(f"Không tìm thấy ảnh với selector {selector}: {e}")  
        
        # Thêm biện pháp phòng ngừa: kiểm tra data-a-dynamic-image  
        if len(img_urls) < max_images:  
            try:  
                dynamic_images = self.driver.find_elements(By.CSS_SELECTOR, '[data-a-dynamic-image]')  
                
                for elem in dynamic_images[:2]:  # Chỉ kiểm tra 2 phần tử đầu tiên  
                    json_data = elem.get_attribute('data-a-dynamic-image')  
                    if json_data:  
                        import json  
                        try:  
                            image_dict = json.loads(json_data)  
                            for img_url in list(image_dict.keys())[:max_images]:  
                                full_img = self.convert_to_fullsize(img_url)  
                                if full_img and full_img not in thumb_set:  
                                    img_urls.append(full_img)  
                                    thumb_set.add(full_img)  
                                    if len(img_urls) >= max_images:  
                                        break  
                        except json.JSONDecodeError:  
                            pass  
            except Exception as e:  
                self.logger.debug(f"Không thể xử lý data-a-dynamic-image: {e}")  

        self.logger.info(f"Tìm thấy {len(img_urls)} ảnh cho ASIN {asin}")  
        return img_urls[:max_images]  # Giới hạn số lượng ảnh  
    
    def get_product_info(self, asin):  
        """Lấy tất cả thông tin sản phẩm"""  
        # Thêm biện pháp bảo vệ: đếm số lần thử lại  
        max_retries = 2  
        current_retry = 0  
        
        while current_retry < max_retries:  
            # Tải trang sản phẩm  
            if not self.load_product_page(asin):  
                current_retry += 1  
                if current_retry < max_retries:  
                    self.logger.info(f"Thử lại lần {current_retry} cho ASIN {asin}...")  
                    # Chờ thời gian dài hơn trước khi thử lại  
                    time.sleep(random.uniform(5, 10))  
                    continue  
                else:  
                    self.logger.error(f"Đã thử {max_retries} lần, không thể tải trang sản phẩm ASIN {asin}")  
                    return {}  
            
            # Nếu tải thành công, thoát khỏi vòng lặp  
            break  
        
        # Lấy các thông tin cơ bản  
        title = self.get_title(asin)  
        if not title:  
            self.logger.error(f"Không tìm thấy tiêu đề sản phẩm ASIN {asin}, có thể trang không tồn tại")  
            return {}  
            
        desc = self.get_description(asin)  
        detailed_desc = self.get_detailed_description(asin)  
        price = self.get_price(asin)  
        brand = self.get_brand(asin)  
        
        # Tạo nội dung HTML cho mô tả  
        body_html = desc  
        if detailed_desc:  
            body_html += '<br><br>' + detailed_desc  
        
        # Lấy hình ảnh sản phẩm  
        images = self.get_images(asin)  
        
        # Tạo handle từ tiêu đề  
        handle = ""  
        if title:  
            handle = self.slugify(title) + "-" + asin[-4:]  
        else:  
            handle = asin  
        
        # Tạo đối tượng thông tin sản phẩm  
        info = {  
            "Handle": handle,  
            "Title": title,  
            "Body (HTML)": body_html,  
            "Vendor": "Amazon",  
            "Brand": brand,  
            "Tags": f"Amazon Import,{asin}",  
            "Variant Grams": "",  
            "Variant Price": price,  
            "Variant Barcode": asin,  
            "Image Src": ','.join(images) if images else "",  
            "Image Position": 1,  
        }  
        
        # Chờ thời gian ngẫu nhiên trước khi tiếp tục để tránh bị chặn  
        time.sleep(random.uniform(3, 7))  
        
        return info

class ShopifyCSVProcessor:  
    """Lớp chịu trách nhiệm xử lý tệp CSV cho Shopify"""  
    
    def __init__(self, logger=None):  
        """Khởi tạo processor"""  
        self.logger = logger or logging.getLogger(__name__)  
        self.amazon_scraper = AmazonScraper(logger=self.logger)  
    
    def __del__(self):  
        """Hủy processor"""  
        if hasattr(self, 'amazon_scraper'):  
            del self.amazon_scraper  
    
    def process_file(self, input_csv_path, sample_template_path):  
        """Xử lý tệp CSV input và xuất ra tệp CSV theo mẫu template"""  
        self.logger.info(f"Bắt đầu xử lý {input_csv_path}")  
        
        # Đọc template và lấy mẫu dòng đầu tiên  
        sample_df = pd.read_csv(sample_template_path)  
        template_row = sample_df.iloc[0].copy()  
        all_columns = list(sample_df.columns)  
        
        # Đọc tệp CSV input  
        asin_df = pd.read_csv(input_csv_path)  
        if 'Variant SKU' not in asin_df.columns:  
            self.logger.error("File input phải có cột 'Variant SKU' (ASIN)")  
            return  
        
        # Lọc ASINs hợp lệ  
        asin_df = asin_df.dropna(subset=['Variant SKU'])  
        asin_df = asin_df[asin_df['Variant SKU'].astype(str).str.strip() != '']  
        
        # Fields sẽ lấy từ Amazon  
        fields_from_amz = [  
            "Handle", "Title", "Body (HTML)", "Tags", "Brand",  
            "Variant Grams", "Variant Price", "Variant Barcode"  
        ]  
        
        # Nhóm các biến thể theo sản phẩm  
        products = defaultdict(list)  
        
        # Lấy thông tin từng ASIN  
        for i, row in asin_df.iterrows():  
            asin_raw = row['Variant SKU']  
            asin = str(asin_raw).strip()  
            self.logger.info(f"Đang xử lý ASIN dòng {i+2}: {asin}")  
            
            # Lấy thông tin sản phẩm từ Amazon  
            amz_info = self.amazon_scraper.get_product_info(asin)  
            if not amz_info or not amz_info.get("Title"):  
                self.logger.warning(f"Không lấy được thông tin cho ASIN {asin}")  
                continue  
            
            # Tạo dòng mới từ template  
            new_row = template_row.copy()  
            
            # Giữ nguyên Variant SKU gốc  
            new_row["Variant SKU"] = row["Variant SKU"]  
            
            # Giữ Vendor gốc nếu có, hoặc mặc định "Amazon"  
            new_row["Vendor"] = row.get("Vendor", "Amazon")  
            
            # Lấy thông tin từ Amazon  
            for field in fields_from_amz:  
                if field in amz_info and field in new_row:  
                    new_row[field] = amz_info.get(field, new_row.get(field, ""))  
            
            # Lưu thông tin vào nhóm sản phẩm  
            handle = new_row["Handle"]  
            products[handle].append({  
                "row": new_row,   
                "asin": asin,   
                "images": amz_info.get("Image Src", "").split(',') if amz_info.get("Image Src") else []  
            })  
            
            self.logger.info(f"✓ Imported ASIN {asin}: {amz_info['Title'][:40]}")  
            time.sleep(random.uniform(2.5, 4))  
        
        # Tạo danh sách các dòng hoàn chỉnh cho CSV output  
        rows_full = []  
        
        # Xử lý ảnh cho từng sản phẩm  
        for handle, product_info in products.items():  
            all_images = []  
            self.logger.info(f"Xử lý ảnh cho sản phẩm Handle={handle}")  
            
            # Gom tất cả ảnh của sản phẩm  
            for item in product_info:  
                images = item["images"]  
                for img in images:  
                    if img and img not in all_images:  
                        all_images.append(img)  
            
            # Gán ảnh cho các biến thể  
            for i, item in enumerate(product_info):  
                row = item["row"]  
                # Thêm biến thể với thông tin đã có  
                if i == 0 and all_images:  # Biến thể đầu tiên nhận tất cả ảnh  
                    for img_index, img_url in enumerate(all_images):  
                        image_row = row.copy()  
                        image_row["Image Src"] = img_url  
                        image_row["Image Position"] = img_index + 1  
                        rows_full.append(image_row)  
                else:  # Các biến thể còn lại không cần ảnh (Shopify sẽ dùng chung ảnh)  
                    row["Image Src"] = ""  
                    row["Image Position"] = ""  
                    rows_full.append(row)  
        
        # Kiểm tra kết quả  
        if not rows_full:  
            self.logger.error("Không có dòng nào được import.")  
            return  
        
        # Tạo DataFrame và lưu ra file CSV  
        out_df = pd.DataFrame(rows_full, columns=all_columns)  
        
        base, ext = os.path.splitext(input_csv_path)  
        output_csv_path = f"{base}_update{ext}"  
        
        out_df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')  
        self.logger.info(f"Đã xuất ra file đúng format: {output_csv_path}")  
        return output_csv_path  


# Hàm chính để xử lý nhiều tệp  
def process_multiple_files(file_pairs):  
    with ThreadPoolExecutor(max_workers=3) as executor:  
        futures = []  
        for input_csv, template_csv in file_pairs:  
            logger = setup_logger(os.path.basename(input_csv))  
            processor = ShopifyCSVProcessor(logger=logger)  
            futures.append(executor.submit(processor.process_file, input_csv, template_csv))  
        
        for future in futures:  
            future.result()  # chờ hoàn tất  
    
    print("Tất cả các file đã xử lý xong.")  


# Sử dụng khi chạy trực tiếp  
if __name__ == "__main__":  
    # Thiết lập logging cơ bản  
    logging.basicConfig(  
        level=logging.INFO,  
        format='%(asctime)s - %(levelname)s - %(message)s'  
    )  
    
    # Xử lý một file đơn  
    asin_input = r"C:\Users\Dellpro\Documents\Shopify products\demo.csv"  
    template_file = r"C:\Users\Dellpro\Documents\Shopify products\products_export\Decor\caytrangtri_export\caytrangtri_export_update.csv"  
    
    # Tạo logger riêng cho tiến trình chính  
    main_logger = setup_logger("main_process")  
    
    # Tạo processor với logger đã thiết lập  
    processor = ShopifyCSVProcessor(main_logger)  
    
    try:  
        # Xử lý file  
        output_file = processor.process_file(asin_input, template_file)  
        main_logger.info(f"Xử lý hoàn tất! Kết quả được lưu tại: {output_file}")  
    except Exception as e:  
        main_logger.error(f"Đã xảy ra lỗi trong quá trình xử lý: {e}", exc_info=True)