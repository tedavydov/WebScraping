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
        if item['item_type'] == 'followed_by':  # подписчики
            url = ItemAdapter(item)['photo_url']
        elif item['item_type'] == 'follow':  # подписки
            url = ItemAdapter(item)['subscribed_to_photo']
        else:
            url = ItemAdapter(item)['photo_url']
        # print(f'DEBUG PhotosPipeline === url == {url}')
        return scrapy.Request(url)

    def item_completed(self, results, item, info):
        image_path = [x['path'] for ok, x in results if ok]
        if not image_path:
            # print(f'DEBUG DropItem / results == {results}')
            raise DropItem("Item contains no images")
        adapter = ItemAdapter(item)
        adapter['photo_save_tmp'] = image_path
        return item

    def file_path(self, request, response=None, info=None, *, item=None):
        image_guid = hashlib.sha1(to_bytes(request.url)).hexdigest()
        if item['item_type'] == 'followed_by':  # подписчики
            file_name = f"{item['user_id']}_{image_guid}.jpg"
        elif item['item_type'] == 'follow':  # подписки
            file_name = f"{item['subscribed_to']}_{image_guid}.jpg"
        return file_name


class SocialnetworksPipeline:

    def __init__(self):
        client = MongoClient('localhost', 27017)
        self.mongo_base = client.social_20210113

    def process_item(self, item, spider):
        # инициируем коллекцию базы данных
        collection = self.mongo_base[spider.name]
        # ============================================
        # подписки / контакты, на которые подписан пользователь item['user_id']
        if item['item_type'] == 'follow':  # подписки
            usr = {}  # контакт на который подписан пользователь item['user_id']
            us_to_id = item['subscribed_to']
            # ----------------------------------------
            for us in collection.find({'_id': us_to_id}):
                usr = us
            if not usr.get('user_id'):
                usr['user_id'] = us_to_id
            usr['username'] = item['subscribed_to_username']
            usr['full_name'] = item['subscribed_to_full_name']
            usr['user_data'] = item['subscribed_to_data']
            usr['photo_url'] = item['subscribed_to_photo']
            usr['photo_save'] = item['photo_save_tmp']
            # ----------------------------------------
            list_tmp = [].append(item['user_id'])
            usr['subscribed_all'] = list_tmp
            # ----------------------------------------
            # обновляем информацию по контакту на который подписан пользователь в базе данных
            collection.update_one({'_id': usr['user_id']}, {'$set': usr}, upsert=True)
            # ----------------------------------------
            us_core = {}
            for us in collection.find({'_id': item['user_id']}):
                us_core = us
            if not us_core.get('user_id'):
                us_core['user_id'] = item['user_id']
                us_core['username'] = item['username']
            # ----------------------------------------
            all = us_core.get('subscribed_all')
            if all:
                set_tmp = set(all)
            else:
                set_tmp = set({})
            set_tmp.add(usr.get('user_id'))
            # usr == контакт на который подписан пользователь item['user_id']
            # ----------------------------------------
            list_tmp = list(set_tmp)
            us_core['subscribed_all'] = list_tmp
            # ----------------------------------------
            # обновляем информацию по контакту item['user_id'] в базе данных
            collection.update_one({'_id': us_core['user_id']}, {'$set': us_core}, upsert=True)
            # ----------------------------------------
        # ============================================
        elif item['item_type'] == 'followed_by':  # подписчики
            del item['item_type']
            item['photo_save'] = item['photo_save_tmp']
            del item['photo_save_tmp']
            if item.get('subscribed_to_photo'):
                del item['subscribed_to_photo']
            if item.get('subscribed_to_username'):
                del item['subscribed_to_username']
            if item.get('subscribed_to_full_name'):
                del item['subscribed_to_full_name']
            if item.get('subscribed_to_data'):
                del item['subscribed_to_data']
            # print('. ' * 50, f'\nDEBUG del item ==> pprint(item):')
            # pprint(item)
            # ----------------------------------------
            # item['user_id'] ==> подписчик контакта item['subscribed_to']
            usr = {}  # подписчик
            for us in collection.find({'_id': item['user_id']}):
                usr = us
            if not usr.get('user_id'):
                usr = item
            # ----------------------------------------
            all = usr.get('subscribed_all')
            if all:
                set_tmp = set(all)
            else:
                set_tmp = set({})
            set_tmp.add(usr.pop('subscribed_to'))
            # set_tmp.add(item.get('subscribed_to'))
            # ----------------------------------------
            list_tmp = list(set_tmp)
            usr['subscribed_all'] = list_tmp
            # обновляем информацию по подписчику ==> контакту item['user_id'] в базе данных
            collection.update_one({'_id': usr['user_id']}, {'$set': usr}, upsert=True)
            # ----------------------------------------
        # ============================================
        else:
            print('=' * 100, f'\nprocess_item ERROR: item[item_type] ==> {item["item_type"]}')
        # ============================================
        return item
