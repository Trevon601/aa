import pandas as pd  
import time  
import random  
import re  
import os  
import unidecode  
import logging  
import sys  
import csv  
from collections import defaultdict  
from concurrent.futures import ThreadPoolExecutor  

from selenium import webdriver  
from selenium.webdriver.chrome.options import Options  
from selenium.webdriver.common.by import By  
from selenium.webdriver.support.ui import WebDriverWait  
from selenium.webdriver.support import expected_conditions as EC  

def setup_logger(name):  
    logger = logging.getLogger(name)  
    logger.setLevel(logging.INFO)  

    if logger.hasHandlers():  
        logger.handlers.clear()  

    formatter = logging.Formatter(f'%(asctime)s - {name} - %(levelname)s - %(message)s')  

    ch = logging.StreamHandler(sys.stdout)  
    ch.setFormatter(formatter)  
    logger.addHandler(ch)  

    return logger  

def slugify(text):  
    text = unidecode.unidecode(text)  
    text = re.sub(r"[^\w\s-]", '', text).strip().lower()  
    return re.sub(r"[-\s]+", '-', text)  

def convert_to_fullsize(img_url):  
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

def get_amazon_images(asin, driver, logger):  
    url = f'https://www.amazon.com/dp/{asin}'  
    driver.get(url)  
    try:  
        wait = WebDriverWait(driver, 15)  
        wait.until(EC.presence_of_element_located((By.ID, "landingImage")))  
        time.sleep(2)  # chờ JS load xong thumbnail  
    except:  
        logger.warning(f"Timeout loading page for ASIN {asin}")  
        return []  

    img_urls = []  

    # Lấy ảnh chính  
    try:  
        main_img = driver.find_element(By.ID, "landingImage")  
        src = main_img.get_attribute('src')  
        if src:  
            main_full = convert_to_fullsize(src)  
            img_urls.append(main_full)  
    except:  
        logger.warning(f"Không tìm thấy ảnh chính của ASIN {asin}")  

    # Lấy các thumbnail (chỉ lấy src, convert sang fullsize, và loại trùng với ảnh chính)  
    thumb_set = set(img_urls)  
    try:  
        thumbnails = driver.find_elements(By.CSS_SELECTOR, '#altImages img')  
        for thumb in thumbnails:  
            thumb_src = thumb.get_attribute('src')  
            if thumb_src:  
                full_img = convert_to_fullsize(thumb_src)  
                if full_img not in thumb_set:  
                    img_urls.append(full_img)  
                    thumb_set.add(full_img)  
    except Exception as e:  
        logger.warning(f"Không tìm thấy ảnh phụ của ASIN {asin}: {e}")  

    logger.info(f"Tìm thấy tối đa {len(img_urls)} ảnh chính và thumbnail cho ASIN {asin}")  
    return img_urls

def get_amazon_info(asin, driver, logger):  
    url = f'https://www.amazon.com/dp/{asin}'  
    driver.get(url)  

    try:  
        wait = WebDriverWait(driver, 15)  
        wait.until(EC.presence_of_element_located((By.ID, "landingImage")))  
        wait.until(EC.presence_of_element_located((By.ID, "productTitle")))  
    except Exception as e:  
        logger.warning(f"Timeout loading main page or title for ASIN {asin}: {e}")  
        return {}  

    # Thêm thời gian chờ tĩnh để toàn bộ phần tử JS tải xong  
    time.sleep(4)  

    title = ''  
    desc = ''  
    detailed_desc = ''  
    price = ''  

    try:  
        title = driver.find_element(By.ID, "productTitle").text.strip()  
    except Exception as e:  
        logger.warning(f"Không lấy được title ASIN {asin}: {e}")  

    try:  
        desc = driver.find_element(By.ID, "feature-bullets").text.strip().replace('\n', '<br>')  
    except:  
        desc = ''  

    try:  
        detailed_desc = driver.find_element(By.ID, "productDescription").text.strip().replace('\n', '<br>')  
    except:  
        detailed_desc = ''  

    # Lấy giá với nhiều cách thử khác nhau do Amazon thay đổi nhiều mẫu hiển thị giá  
    price_selectors = [  
        'span.a-price > span.a-offscreen',  
        '#priceblock_ourprice',  
        '#priceblock_dealprice',  
        '.apexPriceToPay > span[aria-hidden="true"]'  
    ]  
    for sel in price_selectors:  
        try:  
            price_raw = driver.find_element(By.CSS_SELECTOR, sel).text  
            if price_raw:  
                price = price_raw.replace("$", "").replace(",", "").strip()  
                if price:  
                    break  
        except:  
            continue  

    body_html = desc  
    if detailed_desc:  
        body_html += '<br><br>' + detailed_desc  

    # Lấy hình ảnh sản phẩm  
    images = get_amazon_images(asin, driver, logger)  

    info = {  
        "Handle": (slugify(title) + "-" + asin[-4:]) if title else asin,  
        "Title": title,  
        "Body (HTML)": body_html,  
        "Vendor": "Amazon",  
        "Tags": f"Amazon Import,{asin}",  
        "Variant Grams": "",  
        "Variant Price": price,  
        "Variant Barcode": asin,  
        "Image Src": ','.join(images) if images else "",  
        "Image Position": 1,  
    }  
    return info  

def create_driver():  
    chrome_options = Options()  
    chrome_options.add_argument("--headless")  
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')  
    chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])  
    chrome_options.add_experimental_option('useAutomationExtension', False)  
    driver = webdriver.Chrome(options=chrome_options)  
    return driver  

def process_file(input_csv_path, sample_template_path, logger=None):  
    if logger is None:  
        logger = setup_logger(os.path.basename(input_csv_path))  
        
    logger.info(f"Bắt đầu xử lý {input_csv_path}")  
    
    sample_df = pd.read_csv(sample_template_path)  
    template_row = sample_df.iloc[0].copy()  

    asin_df = pd.read_csv(input_csv_path)  
    if 'Variant SKU' not in asin_df.columns:  
        logger.error("File input phải có cột 'Variant SKU' (ASIN)")  
        return  

    asin_df = asin_df.dropna(subset=['Variant SKU'])  
    asin_df = asin_df[asin_df['Variant SKU'].astype(str).str.strip() != '']  

    fields_from_amz = [  
        "Handle", "Title", "Body (HTML)", "Tags",  
        "Variant Grams", "Variant Price",  
        "Variant Barcode"  
    ]  
    all_columns = list(sample_df.columns)  

    driver = create_driver()  
    rows_full = []  

    # Trước tiên, nhóm các biến thể theo sản phẩm  
    products = defaultdict(list)  
    
    for i, row in asin_df.iterrows():  
        asin_raw = row['Variant SKU']  
        asin = str(asin_raw).strip()  
        logger.info(f"Đang xử lý ASIN dòng {i+2}: {asin}")  

        amz_info = get_amazon_info(asin, driver, logger)  
        if not amz_info or not amz_info.get("Title"):  
            logger.warning(f"Không lấy được thông tin cho ASIN {asin}")  
            continue  

        new_row = template_row.copy()  

        # Giữ nguyên Variant SKU gốc  
        new_row["Variant SKU"] = row["Variant SKU"]  

        # Giữ Vendor gốc nếu có, hoặc mặc định "Amazon"  
        new_row["Vendor"] = row.get("Vendor", "Amazon")  

        # Lấy thông tin từ Amazon  
        for field in fields_from_amz:  
            new_row[field] = amz_info.get(field, new_row.get(field, ""))  
            
        # Xử lý hình ảnh riêng  
        handle = new_row["Handle"]  
        products[handle].append({"row": new_row, "asin": asin})  
        
        logger.info(f"✓ Imported ASIN {asin}: {amz_info['Title'][:40]}")  
        time.sleep(random.uniform(2.5, 4))  

    # Xử lý ảnh cho từng sản phẩm  
    for handle, product_info in products.items():  
        all_images = []  
        logger.info(f"Lấy ảnh cho sản phẩm Handle={handle}")  
        
        # Lấy tất cả ảnh cho các biến thể của sản phẩm  
        for item in product_info:  
            asin = item["asin"]  
            time.sleep(random.uniform(1, 2))  
            images = get_amazon_images(asin, driver, logger)  
            if images:  
                # Thêm vào danh sách ảnh chung của sản phẩm  
                for img in images:  
                    if img not in all_images:  
                        all_images.append(img)  
        
        # Gán ảnh cho các biến thể  
        for i, item in enumerate(product_info):  
            row = item["row"]  
            # Thêm biến thể với thông tin đã có và ảnh đầu tiên  
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

    driver.quit()  

    if not rows_full:  
        logger.error("Không có dòng nào được import.")  
        return  

    out_df = pd.DataFrame(rows_full, columns=all_columns)  

    base, ext = os.path.splitext(input_csv_path)  
    output_csv_path = f"{base}_update{ext}"  

    out_df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')  
    logger.info(f"Đã xuất ra file đúng format: {output_csv_path}")  
    return output_csv_path  

def process_multiple_files(file_pairs):  
    with ThreadPoolExecutor(max_workers=3) as executor:  
        futures = []  
        for input_csv, template_csv in file_pairs:  
            logger = setup_logger(os.path.basename(input_csv))  
            futures.append(executor.submit(process_file, input_csv, template_csv, logger))  

        for future in futures:  
            future.result()  # chờ hoàn tất  

    print("Tất cả các file đã xử lý xong.")  

if __name__ == "__main__":  
    # Có thể xử lý một file đơn:  
    asin_input = r"C:\Users\Dellpro\Documents\Shopify products\demo.csv"  
    template_file = r"C:\Users\Dellpro\Documents\Shopify products\products_export\Decor\caytrangtri_export\caytrangtri_export_update.csv"  
    process_file(asin_input, template_file)  
    
    # # Hoặc xử lý nhiều file cùng lúc:  
    # """  
    # file_pairs = [  
    #     (r"C:\Users\Dellpro\Documents\Shopify products\products_export\Pet\quanaochomeo_export\quanaochomeo_export.csv",   
    #      r"C:\Users\Dellpro\Documents\Shopify products\template.csv"),  
    #     (r"C:\Users\Dellpro\Documents\Shopify products\products_export\Pet\dochamsocthu_export\dochamsocthu_export.csv",   
    #      r"C:\Users\Dellpro\Documents\Shopify products\template.csv"),  
    # ]  
    # process_multiple_files(file_pairs)  
    # """