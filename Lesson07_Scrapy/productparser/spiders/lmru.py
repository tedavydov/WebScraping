import scrapy
import json
from scrapy.http import HtmlResponse
from productparser.items import ProductparserItem
from scrapy.loader import ItemLoader


class LmruSpider(scrapy.Spider):
    name = 'lmru'

    def __init__(self, file_json="./search_settings.json"):
        super(LmruSpider, self).__init__()
        # =================================================
        # постараемся задавать минимум настроек в коде,
        # настройки загружаются из файла, при старте паука
        # -------------------------------------------------
        # sett = self.load_settings("./search_settings_debug.json")
        sett = self.load_settings(file_json)
        self.jsettings = sett[0]
        self.start_urls = sett[1]  # добавил генератор urls для leroymerlin.ru
        # self.start_urls = self.jsettings.get('start_urls')
        self.allowed_domains = self.jsettings.get('domains')

    def parse(self, response: HtmlResponse):
        page_links = response.xpath(self.jsettings.get('main'))
        for link in page_links:
            yield response.follow(link, callback=self.parse_links)
        # ========================================================
        next_page = response.xpath(self.jsettings.get('next_page')).extract_first()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def parse_links(self, response: HtmlResponse):
        loader = ItemLoader(item=ProductparserItem(), response=response)
        loader.add_value('_id', response.url)
        loader.add_value('spider', self.name)
        loader.add_xpath('prod_name', self.jsettings.get('prod_name'))
        loader.add_value('link', response.url)
        loader.add_xpath('photos', self.jsettings.get('photos'))
        loader.add_xpath('price', self.jsettings.get('price'))
        loader.add_xpath('price_currency', self.jsettings.get('price_currency'))
        loader.add_xpath('price_primary_unit', self.jsettings.get('price_primary_unit'))
        loader.add_xpath('price_secondary', self.jsettings.get('price_secondary'))
        loader.add_xpath('price_secondary_unit', self.jsettings.get('price_secondary_unit'))
        loader.add_xpath('characteristic_names',
                         self.jsettings.get('characteristics') + self.jsettings.get('characteristic_names'))
        loader.add_xpath('characteristic_values',
                         self.jsettings.get('characteristics') + self.jsettings.get('characteristic_values'))
        yield loader.load_item()

    def load_settings(self, file_name="./search_settings.json"):
        '''
        Считывает настройки паука из файла JSON
        :param file_name: по умолч. "./search_settings.json"
        :return: список стартовых ссылок и параметры поиска для паука
        '''
        urls = []
        search_settings = {}
        try:
            with open(file_name, "r", encoding="utf-8") as json_file:
                ss = json.load(json_file)
            if ss:
                search_settings = ss.get(self.name)
                if search_settings:
                    base_url = search_settings.get('start_urls')
                    search_keys = search_settings.get('search_keys')
                    if search_keys:
                        urls = self.leroy_urls(search_keys, base_url)
                    else:
                        urls.append(base_url)
        # =======================================================================
        except Exception as e:
            print('=' * 100, f'\nload_settings ERROR: Ошибка чтения JSON файла\n{e}')
        # =======================================================================
        return search_settings, urls

    def leroy_urls(self, key, base_url=None):
        '''
        Формирует список линков из переданных параметров
        :param key: ключевое слово (строка) или список ключевых слов
        :param base_url: основная часть линка, не зависящая от ключевых слов
        :return:
        '''
        if base_url and isinstance(base_url, str):
            b_url = base_url
        else:
            b_url = 'https://leroymerlin.ru'
        # ===========================================================================
        url_list = []
        if isinstance(key, str):
            url = b_url + '/search/?q=' + key
            url_list.append(url)
        # ===========================================================================
        elif isinstance(key, list):
            for k in key:
                if k and isinstance(k, str):
                    url = b_url + '/search/?q=' + k
                    url_list.append(url)
        # ===========================================================================
        elif isinstance(key, dict):
            for k in key.keys():
                if k and isinstance(k, str):
                    url = b_url + '/search/?'
                    if key.get(k):
                        for el in key.get(k):
                            if el and isinstance(el, str):
                                url = url + el + '&'
                    url = url + 'q=' + k
                    url_list.append(url)
            # -----------------------------------------------------------------------
            # 'https://leroymerlin.ru/search/?q=обои'
            # 'https://leroymerlin.ru/search/?10837=Красный&q=обои'
            # 'https://leroymerlin.ru/search/?10837=Красный&00240=Натуральные%20материалы&q=обои'
            # 'https://leroymerlin.ru/search/?q=фанера'
            # 'https://leroymerlin.ru/search/?00256=152.5&q=фанера'
        # ===========================================================================
        else:
            print('=' * 100, f'\nleroy_urls ERROR: переданы некорректные параметры'
                             f'\n key == {key}'
                             f'\n type(key) == {type(key)}')
            url_list.append(b_url)
        # ===========================================================================
        return url_list
