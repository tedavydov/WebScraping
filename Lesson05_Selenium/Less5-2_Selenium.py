from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from selenium.webdriver.support.ui import WebDriverWait
import time

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from pymongo import MongoClient

from pprint import pprint

# ================================================================
chrome_options = Options()
chrome_options.add_argument('start-maximized')
driver = webdriver.Chrome(executable_path='./chromedriver.exe', options=chrome_options)

driver.get('https://mvideo.ru/')

# ================================================================
client = MongoClient('127.0.0.1', 27017)
dbc = client['products'].mvideo
try:
    hitcore = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located(
            (By.XPATH, "//div[contains(text(),'Хиты продаж')]/../../../..")))
    butt = hitcore.find_elements(By.XPATH,
                                 "//a[contains(@class,'next-btn') "
                                 "and contains(@class,'arrow-right')]")[1]
    ul = hitcore.find_element_by_class_name('accessories-product-list')
    # ================================================================
    q = 0
    li_set = set({})
    while True:
        butt.click()
        time.sleep(1)
        lis = hitcore.find_elements(By.XPATH, ".//li[@rel]")
        # ==================================================
        i = 0
        product = {}
        set_tmp = set({})
        for li in lis:
            product['_id'] = li.find_element(By.XPATH, "./div").get_attribute('data-productid')
            product['title'] = li.find_element(By.XPATH, ".//h4").get_attribute('title')
            product['link'] = li.find_element(By.XPATH, ".//h4/a").get_attribute('href')
            set_tmp.add(product.get('_id'))
            dbc.update_one({'_id': product.get('_id')}, {'$set': product}, upsert=True)
            i += 1
        # ==========================
        if set_tmp.issubset(li_set):
            break
        li_set.update(set_tmp)
        # ==========================
        q += 1
except Exception as e:
    print(f'Hit ERROR == {e}')
# ================================================================

# вывод базы для проверки
for prod in dbc.find({}):
    pprint(prod)

driver.close()
