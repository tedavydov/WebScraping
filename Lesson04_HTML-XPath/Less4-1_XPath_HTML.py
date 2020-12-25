import requests
from lxml import html
from pprint import pprint
from pymongo import MongoClient


# from sys import argv
# from datetime import datetime as dt
# import pandas as pd

# 1 Для парсинга использовать XPath
def from_xpath(url, xpath):
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
    response = requests.get(url, headers=header)
    dom = html.fromstring(response.text)
    items = []
    if isinstance(xpath, str):
        items = dom.xpath(xpath)
    elif isinstance(xpath, list):
        for x in xpath:
            tmp = dom.xpath(x)
            if tmp:
                items.append(tmp)
    elif isinstance(xpath, dict):
        # print('lnk/url === ', url)
        # print('xpath === ', xpath)
        item0 = dom.xpath(xpath['main'])
        # print('item0 === ', item0)
        if item0 and isinstance(item0, list):
            for it in item0:
                item1 = {}
                for k in xpath.keys():
                    if k != 'main':
                        kp = '.' + xpath[k]
                        fnd = it.xpath(kp)
                        if fnd:
                            item1[k] = fnd[0]
                        else:
                            item1[k] = None
                items.append(item1)
    return items


def news_mail(nws=None):
    url = 'https://news.mail.ru'
    xpath = "//a[contains(@href,'news') and not(contains(@href,'card'))]/@href"
    if nws and isinstance(nws, list):
        result = nws
    else:
        result = []
    items = from_xpath(url, xpath)
    xpaths = {'main': "//div[contains(@class,'article')]/div[contains(@class,'breadcrumbs')]/..",
              'header': "//h1[@class='hdr__inner']/text()",
              'datetime': "//span[contains(@class,'breadcrumbs')]/@datetime",
              'src_link': "//a[contains(@class,'breadcrumbs')]/@href",
              'src_text': "//a[contains(@class,'breadcrumbs')]/span[@class='link__text']/text()"}
    if items:
        for item in items:
            if item[:20] == 'https://news.mail.ru':
                res = from_xpath(item, xpaths)
                if res:
                    res_dict = res[0]
                    res_dict['link'] = item
                    result.append(res_dict)
    return result


def news_lenta(nws=None):
    url = 'https://lenta.ru'
    xpaths = {'main': "//div/a[contains(@href,'news/2020')]",
              'header': "/text()",
              'datetime': "/time/@datetime",
              'link': "/@href"}
    items = from_xpath(url, xpaths)
    result = []
    if items:
        for item in items:
            res = {}
            lnk = item.get('link')
            dt = item.get('datetime')
            if lnk[:5] == '/news':
                lnk = url + item.get('link')
                if not dt:
                    xpath = "//div[@class='b-topic__info']/time/@datetime"
                    dt2 = from_xpath(lnk, xpath)
                    if dt2:
                        res['datetime'] = dt2[0]
                else:
                    res['datetime'] = dt
            hd = item.get('header')
            if hd:
                res['header'] = hd.replace(u'\xa0', u' ')
            res['link'] = lnk
            res['src_link'] = url + '/'
            res['src_text'] = 'Лента.РУ'
            result.append(res)
    if nws and isinstance(nws, list):
        nws.extend(result)
        return nws
    else:
        return result


def news_yandex(nws=None):
    url = 'https://yandex.ru/'
    xpaths = {'main': "//div[contains(@class,'news-story__head')]",
              'header': "/../h1/text()",
              'datetime': "/a/@data-log-id",
              'src_link': "/a/@href",
              'src_text': "//span[contains(@class,'news-story')]/text()"}
    xpath1 = "//ol/li/a/@href"
    items1 = from_xpath(url, xpath1)
    xpath2 = "//div[contains(@class,'divjson2-text')]/span[contains(@class,'i-multiline-overflow')]"
    items2 = from_xpath(url, xpath2)
    itx = {}
    if items1:
        for item in items1:
            itx[item] = None
    if items2:
        for item in items2:
            itx[item] = None
    if itx:
        for lnk in itx.keys():
            res = from_xpath(lnk, xpaths)
            # print('res === ', res)
    if nws and isinstance(nws, list):
        nws.extend(res)
        return nws
    else:
        return res


def to_mongodb(news, clt, ups=False):
    '''
    :param news: список с новостями (словари)
    :param clt: коллекция из БД MongoDB == <class 'pymongo.collection.Collection'>
    :param ups: upsert=True ==> добавление новых записей в БД, False ==> обновление существующих записей
    :return: None
    '''
    if news and isinstance(news, list):
        for new in news:
            # проверка наличия новости / обновление / upsert=True ==> добавление новых записей
            clt.update_many({'link': new['link']}, {'$set': new}, upsert=ups)


# 1 Написать приложение, которое собирает основные новости с сайтов mail.ru, lenta.ru, yandex
news = []
news = news_mail(news)
news = news_lenta(news)
news = news_yandex(news)
# pprint(news)

client = MongoClient('127.0.0.1', 27017)
db = client['news']
collect = db.last

# 2 Сложить все данные в БД
to_mongodb(news, collect, True)

# # вывод всех новостей из БД
# for new in collect.find({}):
#     pprint(new)

# вывод всех новостей от Ленты
for new in collect.find({'src_link': 'https://lenta.ru/'}):
    pprint(new)
