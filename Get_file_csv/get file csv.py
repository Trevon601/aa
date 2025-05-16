import pandas as pd  
import time  
import random  
import re  
import os  
import unidecode  
from selenium import webdriver  
from selenium.webdriver.chrome.options import Options  
from selenium.webdriver.common.by import By  
from selenium.webdriver.support.ui import WebDriverWait  
from selenium.webdriver.support import expected_conditions as EC  

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




def get_amazon_info(asin, driver):  
    url = f'https://www.amazon.com/dp/{asin}'  
    driver.get(url)  
    try:  
        wait = WebDriverWait(driver, 10)  
        wait.until(EC.presence_of_element_located((By.ID, "landingImage")))  
    except:  
        return {}  # Không truy cập được  

    title, desc, price, image = '', '', '', ''  
    try:  
        title = driver.find_element(By.ID, "productTitle").text.strip()  
    except: pass  
    try:  
        desc = driver.find_element(By.ID, "feature-bullets").text.strip().replace('\n', '<br>')  
    except: pass  
    try:  
        price = driver.find_element(By.CSS_SELECTOR, 'span.a-price > span.a-offscreen').text.replace("$", "").replace(",", "")  
    except: pass  
    try:  
        image = driver.find_element(By.ID, "landingImage").get_attribute('src')  
        image = convert_to_fullsize(image)  
    except: image = ""  
    info = {  
        "Handle": (slugify(title) + "-" + asin[-4:]) if title else asin,  
        "Title": title,  
        "Body (HTML)": desc,  
        "Vendor": "Amazon",  
        "Tags": f"Amazon Import,{asin}",  
        "Variant Grams": "",  
        "Variant Price": price,  
        "Variant Barcode": asin,  
        "Image Src": image,  
        "Image Position": 1  
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

def process_file(input_csv_path, sample_template_path):  
    # Đọc file mẫu đầy đủ  
    sample_df = pd.read_csv(sample_template_path)  
    template_row = sample_df.iloc[0].copy()  # Lấy giá trị mặc định từ dòng đầu tiên  

    # Đọc file input  
    asin_df = pd.read_csv(input_csv_path)  
    if 'Variant SKU' not in asin_df.columns:  
        print("File input phải có cột 'Variant SKU' (ASIN)")  
        return  

    # Các trường cần lấy và mapping  
    fields_from_amz = [  
        "Handle", "Title", "Body (HTML)", "Vendor",  
        "Tags", "Variant Grams", "Variant Price",  
        "Variant Barcode", "Image Src", "Image Position"  
    ]  
    all_columns = list(sample_df.columns)  

    driver = create_driver()  

    rows_full = []  
    for i, row in asin_df.iterrows():  
        asin = row.get('Variant SKU')  
        if isinstance(asin, float) and pd.isna(asin):  
            print(f"Bỏ qua dòng {i+1} vì ASIN là NaN")  
            continue  
        asin = str(asin).strip()  
        if not asin:  
            print(f"Đã xử lý hết danh sách ASIN! Dừng chương trình tại dòng {i+1}.")  
            break 
        amz_info = get_amazon_info(asin, driver)  
        if not amz_info or not amz_info.get("Title"):  
            print(f"Không lấy được thông tin cho ASIN {asin}")  
            continue  

        new_row = template_row.copy()  
        for field in fields_from_amz:  
            new_row[field] = amz_info.get(field, "")  
        rows_full.append(new_row)  
        print(f"✓ Imported ASIN {asin}: {amz_info['Title'][:40]}")  
        time.sleep(random.uniform(2.5, 4))

    driver.quit()  

    if not rows_full:  
        print("Không có dòng nào được import.")  
        return  

    out_df = pd.DataFrame(rows_full, columns=all_columns)  

    # Output file cùng tên input, thêm _update  
    base, ext = os.path.splitext(input_csv_path)  
    output_csv_path = f"{base}_update{ext}"  

    out_df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')  
    print(f"Đã xuất ra file đúng format: {output_csv_path}")  

if __name__ == "__main__":  
    # Cập nhật các đường dẫn file phù hợp  
    asin_input = r"C:\Users\Dellpro\Documents\Shopify products\demo.csv"  # hoặc file đang dùng, có cột 'Variant SKU'  
    template_file = r"C:\Users\Dellpro\Documents\Shopify products\products_export\Decor\caytrangtri_export\caytrangtri_export_update.csv"  
    process_file(asin_input, template_file)