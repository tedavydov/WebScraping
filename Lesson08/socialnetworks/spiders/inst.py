import scrapy
import json
from scrapy.http import HtmlResponse
from socialnetworks.items import SocialnetworksItem
import re
from urllib.parse import urlencode
from copy import deepcopy
from scrapy.loader import ItemLoader


class InstSpider(scrapy.Spider):
    name = 'inst'
    base_urls = ['http://instagram.com/']

    def __init__(self, file_json="./search_settings.json"):
        super(InstSpider, self).__init__()
        # =================================================
        # задаем минимум настроек в коде
        # (только имя паука, используемое как ключ для загрузки настроек),
        # настройки загружаются из файла, при старте паука
        # -------------------------------------------------
        # self.jsettings = self.load_settings("./search_settings_debug.json")
        # self.jsettings = self.load_settings("./search_settings_debug.json", "tim")
        # print('=' * 100, f"\nDEBUG jsettings : {self.jsettings}")
        # -------------------------------------------------
        self.jsettings = self.load_settings(file_json)
        self.start_urls = self.jsettings.get('start_urls')
        self.allowed_domains = self.jsettings.get('domains')

    def load_settings(self, file_name="./search_settings.json", option=None):
        '''
        Считывает настройки паука из файла JSON
        :param file_name: по умолч. "./search_settings.json"
        :return: список стартовых ссылок и параметры поиска для паука
        '''
        if option and isinstance(option, str):
            get_key = self.name + option
        else:
            get_key = self.name
        search_settings = {}
        try:
            with open(file_name, "r", encoding="utf-8") as json_file:
                sett = json.load(json_file)
            if sett:
                search_settings = sett.get(get_key)
        # =======================================================================
        except Exception as e:
            print('=' * 100, f'\nload_settings ERROR: Ошибка чтения JSON файла {file_name}\n{e}')
        # =======================================================================
        return search_settings

    def parse(self, response: HtmlResponse):
        csrf_token = self.fetch_csrf_token(response.text)
        # print('=' * 100, f"\nDEBUG csrf_token : {csrf_token}")
        yield scrapy.FormRequest(
            self.jsettings.get('inst_login_link'),
            method='POST',
            callback=self.user_login,
            formdata={'username': self.jsettings.get('inst_login'),
                      'enc_password': self.jsettings.get('inst_password')},
            headers={'X-CSRFToken': csrf_token}
        )

    def user_login(self, response: HtmlResponse):
        j_data = response.json()
        if j_data['authenticated']:
            us = self.jsettings.get('parse_user')
            if us and isinstance(us, list):
                for u in us:
                    yield response.follow(
                        f"/{u}",
                        callback=self.user_data_first_page,
                        cb_kwargs={'username': u}
                    )

    def user_data_first_page(self, response: HtmlResponse, username):
        x_hash = self.jsettings.get('posts_hash')
        if x_hash and isinstance(x_hash, dict):
            for x in x_hash.keys():
                data_title = x
                data_hash = x_hash.get(data_title)
                # ---------------------------------------------------
                user_id = self.fetch_user_id(response.text, username)
                # ---------------------------------------------------
                variables = {'id': user_id, 'include_reel': True, 'fetch_mutual': False, 'first': 12}
                url_posts = f"{self.jsettings.get('graphql_url')}" \
                            f"query_hash={data_hash}" \
                            f"&{urlencode(variables)}"
                yield response.follow(
                    url_posts,
                    callback=self.user_data_next_page,
                    cb_kwargs={'username': username,
                               'user_id': user_id,
                               'data_title': data_title,
                               'data_hash': data_hash,
                               'variables': deepcopy(variables)}
                )

    def user_data_next_page(self, response: HtmlResponse, username, user_id, data_title, data_hash, variables):
        # =====================================================================================
        if data_title and isinstance(data_title, str):
            j_data = response.json()
            # =================================================================================
            if data_title == "Подписчики":
                page_info = j_data.get('data').get('user').get('edge_followed_by').get('page_info')
                if page_info.get('has_next_page'):
                    variables['after'] = page_info.get('end_cursor')
                    url_posts = f"{self.jsettings.get('graphql_url')}" \
                                f"query_hash={data_hash}" \
                                f"&{urlencode(variables)}"
                    yield response.follow(
                        url_posts,
                        callback=self.user_data_next_page,
                        cb_kwargs={'username': username,
                                   'user_id': user_id,
                                   'data_title': data_title,
                                   'data_hash': data_hash,
                                   'variables': deepcopy(variables)}
                    )
                # -----------------------------------------------------------------------------
                # сбор полезной информации по подписчикам:
                # -----------------------------------------------------------------------------
                edges = j_data.get('data').get('user').get('edge_followed_by').get('edges')
                for edge in edges:
                    item = SocialnetworksItem(
                        item_type='followed_by',  # подписчики
                        user_id=edge.get('node').get('id'),
                        username=edge.get('node').get('username'),
                        full_name=edge.get('node').get('full_name'),
                        subscribed_to=user_id,
                        subscribed_to_username=username,
                        photo_url=edge.get('node').get('profile_pic_url'),
                        user_data=edge.get('node')
                    )
                    # =========================================
                    yield item
            # =================================================================================
            elif data_title == "Подписки":
                page_info = j_data.get('data').get('user').get('edge_follow').get('page_info')
                if page_info.get('has_next_page'):
                    variables['after'] = page_info.get('end_cursor')
                    url_posts = f"{self.jsettings.get('graphql_url')}" \
                                f"query_hash={data_hash}" \
                                f"&{urlencode(variables)}"
                    yield response.follow(
                        url_posts,
                        callback=self.user_data_next_page,
                        cb_kwargs={'username': username,
                                   'user_id': user_id,
                                   'data_title': data_title,
                                   'data_hash': data_hash,
                                   'variables': deepcopy(variables)}
                    )
                # -----------------------------------------------------------------------------
                # сбор полезной информации по подпискам :
                # -----------------------------------------------------------------------------
                edges = j_data.get('data').get('user').get('edge_follow').get('edges')
                for edge in edges:
                    item = SocialnetworksItem(
                        item_type='follow',  # подписки
                        user_id=user_id,
                        username=username,
                        subscribed_to=edge.get('node').get('id'),
                        subscribed_to_username=edge.get('node').get('username'),
                        subscribed_to_full_name=edge.get('node').get('full_name'),
                        subscribed_to_photo=edge.get('node').get('profile_pic_url'),
                        subscribed_to_data=edge.get('node')
                    )
                    # =========================================
                    yield item

    # Получаем токен для авторизации
    def fetch_csrf_token(self, text):
        matched = re.search('\"csrf_token\":\"\\w+\"', text).group()
        return matched.split(':').pop().replace(r'"', '')

    # Получаем id желаемого пользователя
    def fetch_user_id(self, text, username):
        matched = re.search('{\"logging_page_id\":\"\\w+\"', text).group() + "}"
        logging_page_id = json.loads(matched).get('logging_page_id')
        id = logging_page_id.replace('profilePage_', '')
        return id

    # выдает ошибку .. не находит id !!!
    # Получаем id желаемого пользователя
    def fetch_user_id_err(self, text, username):
        matched = re.search(
            '{\"id\":\"\\d+\",\"username\":\"%s\"}' % username, text
        ).group()
        return json.loads(matched).get('id')
