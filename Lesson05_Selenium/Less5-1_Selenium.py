from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains

from selenium.webdriver.support.ui import WebDriverWait
import time

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from pymongo import MongoClient

from pprint import pprint

chrome_options = Options()
chrome_options.add_argument('start-maximized')
driver = webdriver.Chrome(executable_path='./chromedriver.exe', options=chrome_options)

driver.get('https://account.mail.ru/login?')

try:
    elem = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located(
            (By.XPATH, "//input[contains(@name,'username')]")))
    elem.send_keys('study.ai_172@mail.ru')
    elem.send_keys(Keys.ENTER)

    elem = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located(
            (By.XPATH, "//input[contains(@name,'password')]")))
    elem.send_keys('NextPassword172')
    elem.send_keys(Keys.ENTER)
except Exception as e:
    print(f'Login ERROR == {e}')

email_links_set = set({})
scroll = 0
while True:
    scroll += 1
    set_tmp = set({})
    try:
        email_links_tmp = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located(
                (By.XPATH, "//div[contains(@class,'dataset__items')]/a")))
        for el in email_links_tmp:
            link = el.get_attribute('href')
            set_tmp.add(link)
            print(f'email_links({scroll}) == {link}')
        print(f'email links page({scroll}) == NEW({len(set_tmp)}) ALL({len(email_links_set)})')
        if set_tmp.issubset(email_links_set):
            break
        email_links_set.update(set_tmp)
        actions = ActionChains(driver)
        actions.move_to_element(email_links_tmp[-1])
        actions.perform()
        time.sleep(1)
    except Exception as e:
        print(f'Links ERROR == {e}')
        break

client = MongoClient('127.0.0.1', 27017)
dbc = client['emails'].mail
for link in email_links_set:
    if link:
        email = {}
        email['_id'] = link[26:46]
        driver.get(link)
        eml_tmp = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located(
                (By.XPATH, "//h2[contains(@class,'thread__subject')]")))
        email['title'] = eml_tmp[0].text
        eml_tmp = driver.find_element(By.XPATH, "//div[contains(@class,'letter__author')]")
        email['from'] = eml_tmp.find_element(By.XPATH, "//span[contains(@class,'letter-contact')]").text
        email['date'] = eml_tmp.find_element(By.XPATH, "//div[contains(@class,'letter__date')]").text
        eml_tmp = driver.find_element(By.XPATH, "//div[contains(@class,'letter__body')]")
        email['text'] = eml_tmp.text
        dbc.update_one({'_id': email['_id']}, {'$set': email}, upsert=True)


# вывод письма из базы для проверки
for email in dbc.find({'_id': '15796784172116936300'}):
    pprint(email)

logout = driver.find_element(By.XPATH, "//a[@id='PH_logoutLink']")
logout.click()
driver.close()
