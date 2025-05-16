class CONFIG:
    # URL cơ sở cho Amazon
    BASE_URL = 'https://www.amazon.com/dp/'
    
    # Thời gian chờ (giây)
    TIMEOUT = 15
    
    # Khoảng thời gian chờ ngẫu nhiên giữa các yêu cầu (giây)
    MIN_DELAY = 2.5
    MAX_DELAY = 4.0
    
    # Đường dẫn mặc định
    DEFAULT_INPUT = r"C:\Users\Dellpro\Documents\Shopify products\demo.csv"
    DEFAULT_TEMPLATE = r"C:\Users\Dellpro\Documents\Shopify products\products_export\Decor\caytrangtri_export\caytrangtri_export_update.csv"
    
    # File checkpoint
    CHECKPOINT_FILE = "checkpoint.json"
    
    # Danh sách proxy (thêm vào nếu có)
    PROXIES = [
        # 'http://proxy1:port',
        # 'http://proxy2:port',
    ]
    
    # Cấu hình selector cho các phần tử trên trang Amazon
    SELECTORS = {
        'product_title': '#productTitle',
        'product_price': '#tp-tool-tip-subtotal-price-value',
        'product_description': '#feature-bullets',
        'image_selectors': [
            {'main': "#landingImage", 'thumbs': "#altImages img"},
            {'main': "#imgBlkFront", 'thumbs': "#imageBlockThumbs img"},
            {'main': ".a-dynamic-image", 'thumbs': ".a-spacing-small.item img"}
        ]
    } 