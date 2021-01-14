# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import hashlib
import scrapy
from itemadapter import ItemAdapter
from scrapy.pipelines.images import ImagesPipeline
from pymongo import MongoClient
from scrapy.utils.python import to_bytes
from pprint import pformat, pprint


# !!! =====================================================
# to_bytes - нужно импортировать именно из scrapy.utils !!!
# from scrapy.utils.python import to_bytes
# ---------------------------------------------------------
# если брать из urllib.parse - не будет работать file_path
# так НЕПРАВИЛЬНО :
# from urllib.parse import urlparse, to_bytes
# !!! =====================================================

class PhotosPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        urls = ItemAdapter(item)['photos']
        # print(f'DEBUG PhotosPipeline === urls == {urls}')
        for url in urls:
            yield scrapy.Request(url)
            # print(f'DEBUG PhotosPipeline === img == {url}')

    def item_completed(self, results, item, info):
        image_paths = [x['path'] for ok, x in results if ok]
        if not image_paths:
            print(f'DEBUG DropItem / results == {results}')
            raise DropItem("Item contains no images")
        adapter = ItemAdapter(item)
        adapter['photos'] = image_paths
        # print(f'DEBUG item_completed / image_paths == {image_paths}')
        return item

    def file_path(self, request, response=None, info=None, *, item=None):
        image_guid = hashlib.sha1(to_bytes(request.url)).hexdigest()
        file_name = f"full/{item['_id']}/{image_guid}.jpg"
        return file_name


class ProductparserPipeline:

    def __init__(self):
        client = MongoClient('localhost', 27017)
        self.mongo_base = client.products_20210111

    def process_item(self, item, spider):
        # =========================================
        print('=' * 100, f'\nDEBUG Product process_item :\n item[_id] ==> {item["_id"]}')
        # =========================================
        tmp = {}
        ch_names = item['characteristic_names']
        ch_values = item['characteristic_values']
        for idx in range(len(ch_names)):
            tmp[ch_names[idx]] = ch_values[idx]
        # ----------------------------------------
        if tmp:
            item['characteristics'] = tmp
            # Удаляем ненужные поля в item
            del item['characteristic_names']
            del item['characteristic_values']
        # =========================================
        # print('DEBUG Product process_item === item[characteristics]')
        # pprint(item["characteristics"])
        # =========================================
        # здесь добавляем в базу данных
        collection = self.mongo_base[spider.name]
        collection.update_one({'_id': item['_id']}, {'$set': item}, upsert=True)
        return item
