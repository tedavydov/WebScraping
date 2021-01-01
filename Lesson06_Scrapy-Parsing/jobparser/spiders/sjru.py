import scrapy
import json
from scrapy.http.response.html import HtmlResponse
from jobparser.items import JobparserItem


class SjruSpider(scrapy.Spider):
    name = 'sjru'
    allowed_domains = ['superjob.ru']

    # start_urls = ['http://superjob.ru/']

    def __init__(self, file_json="./search_settings.json"):
        super(SjruSpider, self).__init__()
        res = self.load_urls(file_json)
        self.start_urls = res[0]
        self.start_param = res[1:]

    def parse(self, response: HtmlResponse):
        vacancies_links = response.xpath(self.start_param[0]).extract()

        for link in vacancies_links:
            yield response.follow(link, callback=self.vacancy_parse)

        next_page = response.xpath(self.start_param[1]).extract_first()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def vacancy_parse(self, response: HtmlResponse):
        link = response.url
        end = link.find('.html')
        if end >= 0:
            start = end - 8
            id = link[start:end]
        else:
            start = 0
            id = link[start:]
        site = self.allowed_domains[0]
        # ==================================
        vacancy_name = response.xpath(self.start_param[2]).extract_first()
        salary = response.xpath(self.start_param[3]).extract()
        company_name = response.xpath(self.start_param[4]).extract()
        company_address = response.xpath(self.start_param[5]).extract()
        # ==================================
        # yield JobparserItem(prof=profession, vacancy_name=vacancy_name, salary=salary, link=link, site=site,
        #                             company_name=company_name, company_address=company_address)
        yield JobparserItem(_id=id, vacancy_name=vacancy_name, salary=salary, link=link, site=site,
                            company_name=company_name, company_address=company_address)

    def load_urls(self, file_name="./search_settings.json"):
        '''
        Считывает настройки паука из файла JSON
        :param file_name: по умолч. "search_settings.json"
        :return: список стартовых ссылок и параметры поиска для паука
        '''
        urls = []
        search_settings = {}
        ss = None
        # ==================================
        start_xpath = ''
        next_page_xpath = ''
        name_xpath = ''
        salary_xpath = ''
        company_name_xpath = ''
        company_address_xpath = ''
        # ==================================
        try:
            with open(file_name, "r", encoding="utf-8") as json_file:
                ss = json.load(json_file)

            if ss:
                search_settings = ss.get('sjru')
                start_xpath = search_settings.get('start')
                next_page_xpath = search_settings.get('next_page')
                name_xpath = search_settings.get('name')
                salary_xpath = search_settings.get('salary')
                company_name_xpath = search_settings.get('company_name')
                company_address_xpath = search_settings.get('company_address')
                main_url = search_settings.get('main_url')
                prof_s = search_settings.get('profession')
            else:
                main_url = 'superjob.ru/vacancy/search/?'
                prof_s = ['Python']
            # ==================================
            prof_set = set({})
            if isinstance(prof_s, list):
                for pr in prof_s:
                    if pr and isinstance(pr, str):
                        prof_set.add(pr.lower())
            # ===========================================================================
            city_set = set({})
            cities = search_settings.get('cities')
            if isinstance(cities, list):
                for city in cities:
                    ci = None
                    city_str = str(city).lower()
                    if city_str == 'moscow':
                        ci = '4'
                    elif city_str == 'москва':
                        ci = '4'
                    elif city_str == 'питер':
                        ci = 'spb'
                    elif city_str.find('петербург') >= 0:
                        ci = 'spb'
                    elif city_str.find('petersburg') >= 0:
                        ci = 'spb'
                    elif city_str.find('московская') >= 0:
                        ci = 'mo'
                    elif city_str == 'мо':
                        ci = 'mo'
                    elif city_str == 'раменское':
                        ci = 'ramenskoe'
                    elif city_str == 'ramenskoe':
                        ci = 'ramenskoe'
                    elif city_str.find('russian') >= 0:
                        ci = 'russia'
                    elif city_str.find('российская') >= 0:
                        ci = 'russia'
                    elif city_str == 'россия':
                        ci = 'russia'
                    elif city_str == 'рф':
                        ci = 'russia'
                    if ci:
                        city_set.add(ci)
            # ===========================================================================
            url_tmp = 'https://www.' + main_url + 'keywords=python&noGeo=1'
            if city_set or prof_set:
                if prof_set:
                    for p in prof_set:
                        par_text = 'keywords=' + str(p)
                        if city_set:
                            for c in city_set:
                                if isinstance(c, int):
                                    url_tmp = 'https://www.' + main_url \
                                              + par_text \
                                              + 'geo=' + str(c)
                                    urls.append(url_tmp)
                                elif isinstance(c, str):
                                    url_tmp = 'https://' + str(c) + '.' \
                                              + main_url \
                                              + par_text
                                    urls.append(url_tmp)
                        else:
                            url_tmp = 'https://www.' + main_url \
                                      + par_text + '&noGeo=1'
                            urls.append(url_tmp)
            # ===========================================================================
            else:
                urls.append(url_tmp)
            # ===================================================================
            return urls, start_xpath, next_page_xpath, name_xpath, salary_xpath, \
                   company_name_xpath, company_address_xpath
            # ===========================================================================
        except Exception as e:
            print('=' * 100, f'\nload_urls ERROR: Ошибка чтения JSON файла\n{e}')
