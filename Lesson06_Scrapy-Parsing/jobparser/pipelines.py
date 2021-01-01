# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from pymongo import MongoClient
import re


class JobparserPipeline:

    def __init__(self):
        client = MongoClient('localhost', 27017)
        self.mongo_base = client.vacancies_20210101

    def process_item(self, item, spider):
        item['vacancy_name'] = self.process_string(item['vacancy_name'])
        item['company_name'] = self.process_string(item['company_name'])
        item['company_address'] = self.process_string(item['company_address'])
        salary_res = self.process_salary(item['salary'])
        item['salary_min'] = salary_res.get('min')
        item['salary_max'] = salary_res.get('max')
        item['salary_currency'] = salary_res.get('val')
        item['salary_comment'] = salary_res.get('comment')
        del item['salary']  # Удаляем ненужное поле в item
        collection = self.mongo_base[spider.name]
        collection.update_one({'_id': item['_id']}, {'$set': item}, upsert=True)
        return item

    def process_string(self, substring):
        '''
        Удаляет из переданной строки непечатные символы
        и прочий "мусор" ...
        :param substrng: входящая строка или список подстрок
        :return: результат в виде строки
        '''

        substring_res = ''
        if isinstance(substring, str):
            a = re.sub('\<.*?\>', ' ', substring, count=0)
            b = re.sub('&nbsp;', ' ', a, count=0)
            lst = b.split()
        elif isinstance(substring, list):
            lst = substring
        # ===========================================
        else:  # isinstance(substrng,...) == NOT list or str
            return substring_res
        # ========= sub_lst =========================
        sub_lst = []
        for s in lst:
            a0 = re.sub('\<.*?\>', ' ', s, count=0)
            b0 = re.sub('&nbsp;', ' ', a0, count=0)
            a = b0.replace('.', ' ')
            b = a.replace(',', ' ')
            ss = b.replace(u'\xa0', u' ').split()
            for x in ss:
                if x:
                    xx = x.strip()
                    sub_lst.append(xx)
        # ========= sub_lst =========================
        flag_s = True
        # считаем что строка не должна начинаться с числа (даже в названии фирмы )
        for s in sub_lst:
            if flag_s:
                if isinstance(s, str):
                    substring_res = s
                    flag_s = False
            else:
                substring_res = substring_res + ' ' + str(s)
        return substring_res

    def process_salary(self, salary):
        '''
        Парсит переданную строку зарплаты
        :param salary: текст содержащий данные по зарплате
        :return: выдает словарь - минимальную, максимальную зарплату (целые числа) и валюту ..
        '''

        salary_dict = {}
        # ===========================================
        if isinstance(salary, str):
            lst = salary.split()
        elif isinstance(salary, list):
            lst = salary
        # ===========================================
        else:  # isinstance(salary,...) == NOT list or str
            salary_dict = {'min': None, 'max': None, 'val': None}
            return salary_dict
        # ========= sub_lst =========================
        sub_lst = []
        for s in lst:
            a = self.process_string(s)
            ss = a.split()
            for x in ss:
                if x:
                    l = x.lower()
                    sub_lst.append(l)
        # ========= sub_lst =========================
        key = 'min'
        val_str = ""
        flag_d_min = True
        flag_d_max = True
        x_prev = None
        for s in sub_lst:
            x = s.strip()
            if (x in ['до', '-', '—']) and (key == 'min'):
                key = 'max'
            # ===============
            else:
                try:  # если x - это число
                    d = int(x)
                    if flag_d_min and (key == 'min'):
                        salary_dict[key] = d
                        flag_d_min = False
                    elif flag_d_max and (key == 'max'):
                        salary_dict[key] = d
                        flag_d_max = False
                    else:
                        salary_dict[key] = (salary_dict.get(key) * 1000) + d
                except (ValueError, TypeError):  # текущий x - не число
                    if (len(x) <= 4) and not (x in ['от', 'до', 'по', 'не', 'з/п', 'на', 'руки', '/', '-', '—']):
                        # элемент валюты не более 4 знаков
                        key = 'val'
                        if (x == 'руб.') or (x == 'руб'):
                            if val_str:
                                val_str = val_str + ' руб'
                            else:
                                val_str = 'руб'
                        else:
                            val_str = x
                        salary_dict[key] = val_str
                    else:  # len(x) > 4  и x - не число, int(x) - выдало ошибку
                        # текущий x - скорее всего не валюта, пишем в комментарий
                        if not (x in ['от', 'руб.', 'руб', 'usd', 'eur', '-', '—']):
                            if salary_dict.get('comment'):
                                salary_dict['comment'] = salary_dict.get('comment') + ' ' + x
                            else:  # комментарий пустой
                                try:  # проверим что предыдущий x - это число
                                    int(x_prev)
                                    salary_dict['comment'] = x
                                except (ValueError, TypeError):  # предыдущий x - не число
                                    if x_prev and not (x_prev in ['от', 'руб.', 'руб', 'usd', 'eur', '-', '—']):
                                        salary_dict['comment'] = x_prev + ' ' + x
                                    else:
                                        salary_dict['comment'] = x
            x_prev = x
        # = # ======== for s in sub_lst ==================
        # ================================================
        if not (salary_dict.get('min')): salary_dict['min'] = None
        if not (salary_dict.get('max')): salary_dict['max'] = None
        if salary_dict.get('val'):
            salary_dict['val'] = re.sub('[-\s.,—]*', '', salary_dict.get('val'), count=0)
        else:
            salary_dict['val'] = None
        # ================================================
        return salary_dict
