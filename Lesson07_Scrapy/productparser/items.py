# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from pprint import pformat, pprint
import re
import scrapy
from itemloaders.processors import MapCompose, TakeFirst


# !!! ==============================================================
# так ПРАВИЛЬНО :
# from itemloaders.processors import MapCompose, TakeFirst
# ------------------------------------------------------------------
# ScrapyDeprecationWarning:
#   scrapy.loader.processors.TakeFirst is deprecated,
#   instantiate itemloaders.processors.TakeFirst instead.
#   price_secondary_unit = scrapy.Field(output_processor=TakeFirst())
# ------------------------------------------------------------------
# УСТАРЕВШИЙ КОД !!! снимается с поддержки :
# from scrapy.loader.processors import MapCompose, TakeFirst
# !!! ==============================================================


def url_to_id(from_url):
    start = from_url.find('/product/')
    if start >= 0:
        start = start + 9
    else:
        start = 0
    # =============================
    end = from_url.find('/', start)
    if end >= 0:
        id = from_url[start:end]
    else:
        id = from_url[start:]
    return id


def id_from_url(url):
    if isinstance(url, str):
        id = url_to_id(url)
    elif isinstance(url, list):
        id = url_to_id(url[0])
    else:
        id = url
    return id


def str_clear(in_str):
    if isinstance(in_str, str):
        # ==================================
        x = in_str.replace('\n', ' ')
        x = x.strip()
        try:
            return float(x)
        except:
            try:
                y = x.replace(' ', '')
                return int(y)
            except:
                return x
    elif isinstance(in_str, list):
        tmp = []
        for s in in_str:
            x = str_clear(s)
            tmp.append(x)
        return tmp
    else:
        return in_str


def process_transit(itm):
    # print(f'=== DEBUG Item === process_transit ==>')
    # pprint(itm)
    return itm


class ProductparserItem(scrapy.Item):
    _id = scrapy.Field(input_processor=MapCompose(id_from_url), output_processor=TakeFirst())
    spider = scrapy.Field(output_processor=TakeFirst())
    prod_name = scrapy.Field(output_processor=TakeFirst())
    link = scrapy.Field(output_processor=TakeFirst())
    price = scrapy.Field(input_processor=MapCompose(str_clear), output_processor=TakeFirst())
    price_currency = scrapy.Field(output_processor=TakeFirst())
    price_primary_unit = scrapy.Field(output_processor=TakeFirst())
    price_secondary = scrapy.Field(input_processor=MapCompose(str_clear), output_processor=TakeFirst())
    price_secondary_unit = scrapy.Field(output_processor=TakeFirst())
    photos = scrapy.Field(input_processor=MapCompose(process_transit))
    characteristics = scrapy.Field()
    characteristic_names = scrapy.Field(input_processor=MapCompose(str_clear))
    characteristic_values = scrapy.Field(input_processor=MapCompose(str_clear))
