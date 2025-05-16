import pandas as pd  
import time  
import random  
import re  
import os  
import unidecode  
import logging  

from selenium import webdriver  
from selenium.webdriver.chrome.options import Options  
from selenium.webdriver.common.by import By  
from selenium.webdriver.support.ui import WebDriverWait  
from selenium.webdriver.support import expected_conditions as EC  

from config import CONFIG

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

def create_driver():  
    chrome_options = Options()  
    chrome_options.add_argument("--headless")  
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')  
    chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])  
    chrome_options.add_experimental_option('useAutomationExtension', False)  
    driver = webdriver.Chrome(options=chrome_options)  
    return driver  

def get_amazon_images(asin, driver, logger):
    selectors = [
        {'main': "#landingImage", 'thumbs': "#altImages img"},
        {'main': "#imgBlkFront", 'thumbs': "#imageBlockThumbs img"},
        {'main': ".a-dynamic-image", 'thumbs': ".a-spacing-small.item img"}
    ]
    
    img_urls = []
    
    for selector_set in selectors:
        try:
            # Thử lấy ảnh chính
            main_elements = driver.find_elements(By.CSS_SELECTOR, selector_set['main'])
            for el in main_elements:
                src = el.get_attribute('src')
                if src:
                    img_urls.append(convert_to_fullsize(src))
            
            # Thử lấy ảnh nhỏ
            thumb_elements = driver.find_elements(By.CSS_SELECTOR, selector_set['thumbs'])
            for el in thumb_elements:
                src = el.get_attribute('src')
                if src and not re.search(r'_CB\d+.*_FMpng_RI_', src):
                    img_urls.append(convert_to_fullsize(src))
                    
            # Nếu tìm thấy ít nhất một ảnh, thoát vòng lặp
            if img_urls:
                break
        except Exception as e:
            logger.debug(f"Selector không phù hợp {selector_set}: {e}")
            continue
    
    # Loại bỏ trùng lặp
    return list(dict.fromkeys(img_urls))

def get_amazon_info(asin, driver, logger):  
    url = f"{CONFIG['base_url']}{asin}"  
    driver.get(url)  
    try:  
        wait = WebDriverWait(driver, CONFIG['timeout'])  
        wait.until(EC.presence_of_element_located((By.ID, "productTitle")))  
        wait.until(EC.presence_of_element_located((By.ID, "tp-tool-tip-subtotal-price-value")))  
        wait.until(EC.presence_of_element_located((By.ID, "feature-bullets")))  
        wait.until(EC.presence_of_element_located((By.ID, "landingImage")))  
        wait.until(EC.presence_of_element_located((By.ID, "altImages")))  
    except Exception as e:  
        logger.warning(f"Timeout load trang chính cho ASIN {asin}: {e}")  
        return {}  

    time.sleep(random.uniform(CONFIG['min_delay'], CONFIG['max_delay']))  # đợi trang tải hết nội dung  

    # Lấy title  
    try:  
        title = driver.find_element(By.ID, "productTitle").text.strip()  
    except Exception as e:  
        logger.warning(f"Không lấy được title ASIN {asin}: {e}")  
        title = ''  

    # Lấy giá  
    try:  
        price_raw = driver.find_element(By.ID, "tp-tool-tip-subtotal-price-value").text.strip()  
        price = price_raw.replace("$", "").replace(",", "").strip()  
    except Exception as e:  
        logger.warning(f"Không lấy được giá ASIN {asin}: {e}")  
        price = ''  

    # Lấy mô tả chi tiết  
    try:  
        detailed_desc = driver.find_element(By.ID, "feature-bullets").text.strip().replace('\n', '<br>')  
    except Exception as e:  
        logger.warning(f"Không lấy được mô tả chi tiết ASIN {asin}: {e}")  
        detailed_desc = ''  

    body_html = detailed_desc  

    # Lấy toàn bộ ảnh (ảnh chính + thumbnails)  
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

def process_file(input_csv_path, sample_template_path, logger):  
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
        "Image Src", "Image Position"  
    ]  
    all_columns = list(sample_df.columns)  

    driver = create_driver()  
    rows_full = []  

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

        # Giữ Variant Barcode gốc hoặc lấy từ amz_info  
        new_row["Variant Barcode"] = row.get("Variant Barcode", amz_info.get("Variant Barcode", ""))  

        for field in fields_from_amz:  
            new_row[field] = amz_info.get(field, new_row.get(field, ""))  

        rows_full.append(new_row)  
        logger.info(f"✓ Imported ASIN {asin}: {amz_info['Title'][:40]}")  
        time.sleep(random.uniform(CONFIG['min_delay'], CONFIG['max_delay']))  

    driver.quit()  

    if not rows_full:  
        logger.error("Không có dòng nào được import.")  
        return  

    out_df = pd.DataFrame(rows_full, columns=all_columns)  

    base, ext = os.path.splitext(input_csv_path)  
    output_csv_path = f"{base}_update{ext}"  

    out_df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')  
    logger.info(f"Đã xuất ra file đúng format: {output_csv_path}")  

if __name__ == "__main__":  
    logging.basicConfig(  
        level=logging.INFO,  
        format='%(asctime)s - %(levelname)s - %(message)s'  
    )  
    logger = logging.getLogger()  

    asin_input = r"C:\Users\Dellpro\Documents\Shopify products\demo.csv"  
    template_file = r"C:\Users\Dellpro\Documents\Shopify products\products_export\Decor\caytrangtri_export\caytrangtri_export_update.csv"  
    process_file(asin_input, template_file, logger)