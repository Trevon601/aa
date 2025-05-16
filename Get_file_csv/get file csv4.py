import pandas as pd  
import time  
import random  
import re  
import os  
import sys  
import json  
import unidecode  
import numpy as np  
import logging  
from logging.handlers import RotatingFileHandler  
from collections import defaultdict  
from concurrent.futures import ThreadPoolExecutor, as_completed  
from selenium import webdriver  
from selenium.webdriver.chrome.options import Options  
from selenium.webdriver.chrome.service import Service  
from selenium.webdriver.common.by import By  
from selenium.webdriver.support.ui import WebDriverWait  
from selenium.webdriver.support import expected_conditions as EC  

def setup_logger(name, level=logging.INFO, log_dir="logs"):  
    """Thiết lập logger"""  
    if not os.path.exists(log_dir):  
        os.makedirs(log_dir)  
    
    logger = logging.getLogger(name)  
    logger.setLevel(level)  
    
    # Thiết lập file log với RotatingFileHandler  
    handler = RotatingFileHandler(os.path.join(log_dir, f"{name}.log"), maxBytes=5*1024*1024, backupCount=3)  
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')  
    handler.setFormatter(formatter)  
    
    logger.addHandler(handler)  
    return logger  

def parse_proxy(proxy_string):  
    """Phân tích chuỗi proxy có định dạng 'ip:port:username:password'"""  
    parts = proxy_string.strip().split(':')  
    if len(parts) == 4:  
        ip, port, username, password = parts  
        return {  
            'http': f'http://{username}:{password}@{ip}:{port}',  
            'https': f'http://{username}:{password}@{ip}:{port}'  
        }  
    elif len(parts) == 2:  
        ip, port = parts  
        return {  
            'http': f'http://{ip}:{port}',  
            'https': f'http://{ip}:{port}'  
        }  
    return None  

class AmazonScraper:  
    """Lớp chịu trách nhiệm scrape thông tin sản phẩm từ Amazon"""  
    
    def __init__(self, logger=None, proxies=None):  
        self.logger = logger or logging.getLogger(__name__)  
        self.driver = self._create_driver()  
        self.proxies = proxies or []  
        self.current_proxy_index = 0  
        
    def _create_driver(self):  
        """Tạo và trả về WebDriver Selenium với các tùy chọn để tránh phát hiện"""  
        chrome_options = Options()  
        chrome_options.add_argument("--headless")  
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')  
        user_agent = random.choice(['Mozilla/5.0 (Windows NT 10.0; Win64; x64)', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'])  
        chrome_options.add_argument(f'user-agent={user_agent}')  
        
        # Thêm proxy nếu có  
        if self.proxies:  
            proxy = self.proxies[self.current_proxy_index]  
            chrome_options.add_argument(f'--proxy-server={proxy}')  
        
        driver = webdriver.Chrome(options=chrome_options)  
        self.logger.info("WebDriver đã khởi tạo thành công")  
        return driver  
    
    def get_product_info(self, asin):  
        """Lấy tất cả thông tin sản phẩm"""  
        url = f'https://www.amazon.com/dp/{asin}'  
        self.driver.get(url)  
        time.sleep(random.uniform(2, 4))  
        
        title = self.driver.find_element(By.CSS_SELECTOR, "span#productTitle").text.strip()  
        price = self.driver.find_element(By.CSS_SELECTOR, "span#priceblock_ourprice").text.strip()  
        # Lấy mô tả ngắn  
        description = self.driver.find_element(By.CSS_SELECTOR, "#feature-bullets").text.strip()  

        return {  
            "Title": title,  
            "Price": price,  
            "Description": description  
        }  

    def __del__(self):  
        """Hủy scraper, đóng driver nếu do scraper tạo"""  
        self.driver.quit()  

class ShopifyCSVProcessor:  
    """Lớp chịu trách nhiệm xử lý tệp CSV cho Shopify"""  

    def __init__(self, logger=None, proxies=None):  
        self.logger = logger or logging.getLogger(__name__)  
        self.amazon_scraper = AmazonScraper(logger=self.logger, proxies=proxies)  
    
    def process_file(self, input_csv_path, output_csv_path):  
        """Xử lý tệp CSV input và xuất ra tệp CSV theo mẫu template"""  
        self.logger.info(f"Bắt đầu xử lý {input_csv_path}")  

        try:  
            asin_df = pd.read_csv(input_csv_path)  
            results = []  

            for _, row in asin_df.iterrows():  
                asin = row['Variant SKU']  
                self.logger.info(f"Xử lý ASIN: {asin}")  

                product_info = self.amazon_scraper.get_product_info(asin)  
                results.append(product_info)  

            output_df = pd.DataFrame(results)  
            output_df.to_csv(output_csv_path, index=False)  

            self.logger.info(f"Đã xuất ra file: {output_csv_path}")  
            return output_csv_path  
        
        except Exception as e:  
            self.logger.error(f"Lỗi khi xử lý file {input_csv_path}: {e}", exc_info=True)  
            return None  

if __name__ == "__main__":  
    import argparse  

    # Thiết lập command line arguments  
    parser = argparse.ArgumentParser(description='Amazon to Shopify CSV Processor')  
    parser.add_argument('--input', help='Đường dẫn tới file CSV chứa ASINs')  
    parser.add_argument('--output', help='Đường dẫn tới file CSV xuất')  
    parser.add_argument('--log-dir', default='logs', help='Thư mục chứa file log')  

    args = parser.parse_args()  

    # Thiết lập logging cơ bản  
    logging.basicConfig(  
        level=logging.INFO,  
        format='%(asctime)s - %(levelname)s - %(message)s'  
    )  

    # Tạo thư mục log nếu chưa tồn tại  
    if not os.path.exists(args.log_dir):  
        os.makedirs(args.log_dir)  
    
    # Tạo logger cho chính  
    logger = setup_logger("MainProcessor", log_dir=args.log_dir)  

    # Tạo processor và xử lý file  
    processor = ShopifyCSVProcessor(logger=logger)  
    output_file = processor.process_file(args.input, args.output)