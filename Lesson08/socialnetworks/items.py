# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class SocialnetworksItem(scrapy.Item):
    # define the fields for your item here like:
    user_id = scrapy.Field()  # id пользователя
    username = scrapy.Field()  # логин пользователя
    full_name = scrapy.Field()  # полное имя пользователя
    photo_url = scrapy.Field()
    photo_save = scrapy.Field()
    user_data = scrapy.Field()  # id пользователя
    subscribed_all = scrapy.Field()  # подписан на пользователей (список всех id)
    subscribed_to = scrapy.Field()  # подписан на пользователя (последняя добавленная запись)
    # =========== временные данные для обработки
    item_type = scrapy.Field()  # подписки или подписчики
    photo_save_tmp = scrapy.Field()
    subscribed_to_username = scrapy.Field()
    subscribed_to_full_name = scrapy.Field()
    subscribed_to_photo = scrapy.Field()
    subscribed_to_data = scrapy.Field()

