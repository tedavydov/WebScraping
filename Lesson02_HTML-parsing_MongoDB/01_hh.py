from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
# from pprint import pformat, pprint

def compensation_to_int(read):
    '''Парсит переданную строку зарплаты,
       выдает словарь - минимальную, максимальную зарплату (целые числа) и валюту ..
    '''

    def str_to_int(sub_str):
        res = {}
        key = None
        val_str = ''
        sub_lst = sub_str.split()
        for el in sub_lst:
            if el == 'от':
                key = 'min'
            elif el == 'до':
                key = 'max'
            elif el == '000':
                x = res.get(key)
                if x:
                    res[key] = x * 1000
            else:
                try:
                    res[key] = int(el)
                except:
                    key = 'val'
                    if el == 'руб.':
                        if val_str != "":
                            val_str = val_str + ' руб'
                        else:
                            val_str = 'руб'
                    else:
                        val_str = el
                    res[key] = val_str
        return res

    zp = {}
    if isinstance(read, str):
        li0 = read.split('-')
        x1 = str_to_int(li0[0])
        x1_None = x1.get(None)
        x1_val = x1.get('val')
        if x1_None:
            zp['min'] = x1_None
            if x1_val: zp['val'] = x1_val
        elif x1:
            zp.update(x1)
        else:
            zp['min'] = x1_None
        if len(li0) > 1:
            x2 = str_to_int(li0[1])
            x2_None = x2.get(None)
            x2_val = x2.get('val')
            if x2_None:
                zp['max'] = x2_None
                if x2_val: zp['val'] = x2_val
            elif x2:
                zp.update(x2)
            else:
                zp['max'] = x2_None
    elif isinstance(read, int):
        zp['max'] = read
    else:
        print(f'ZP ERROR ({read})')
    if not (zp.get('val')): zp['val'] = None
    return zp


def go_to_link(lnk, par, head):
    page_next_url = None
    response = requests.get(lnk, params=par, headers=head)
    soup = bs(response.text, 'html.parser')

    vacancy_names = []
    vacancy_compensations = []
    vacancy_links = []
    vacancy_employers = []
    vacancy_addresses = []
    # vacancy_keys = []

    if response.ok:
        vacancies = soup.findAll('div', {'class': 'HH-VacancySidebarTrigger-Vacancy'})
        # i = 1
        for vacancy in vacancies:
            vacancy_a = vacancy.find('a', {'class': 'HH-LinkModifier'})
            vacancy_names.append(vacancy_a.text)
            vacancy_links.append(vacancy_a['href'])
            vacancy_addresses.append(vacancy.find('span', {'data-qa': "vacancy-serp__vacancy-address"}).text)
            vacancy_employer = vacancy.find('a', {'data-qa': "vacancy-serp__vacancy-employer"})
            if vacancy_employer:
                vacancy_employers.append(vacancy_employer.text)
            else:
                vacancy_employers.append(None)
            vacancy_compensation = vacancy.find('span', {'data-qa': "vacancy-serp__vacancy-compensation"})
            if vacancy_compensation:
                vacancy_compensations.append(compensation_to_int(vacancy_compensation.text))
            else:
                vacancy_compensations.append({'min': None, 'max': None, 'val': None})
            # i += 1

        page_next = soup.findAll('a', {'class': 'HH-Pager-Controls-Next'})
        if page_next:
            page_next_url = main_link + page_next[0]['href']
            # print(page_next_url)
        return page_next_url, vacancy_names, vacancy_compensations, vacancy_links, vacancy_employers, vacancy_addresses


main_link = 'https://hh.ru'
params = {'L_save_area': 'true',
          'clusters': 'true',
          'enable_snippets': 'true',
          'text': 'инженер',
          'showClusters': 'false'}

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:84.0) Gecko/20100101 Firefox/84.0'}

vacancy_names = []
vacancy_compensations = []
vacancy_links = []
vacancy_employers = []
vacancy_addresses = []
# vacancy_keys = []

link = f'{main_link}/search/vacancy'
while link:
    print(link)
    rez = go_to_link(link, params, headers)
    vacancy_names.extend(rez[1])
    vacancy_compensations.extend(rez[2])
    vacancy_links.extend(rez[3])
    vacancy_employers.extend(rez[4])
    vacancy_addresses.extend(rez[5])
    # vacancy_keys.extend(rez[x])
    link = rez[0]

vacancies_df = pd.DataFrame({
    'name': vacancy_names,
    'compensation': vacancy_compensations,
    'link': vacancy_links,
    'employer': vacancy_employers,
    'address': vacancy_addresses,
    'site': main_link
})

vacancies_df.index.name = 'num'
vacancies_df.to_csv('vacancies.csv')

print(vacancies_df)
