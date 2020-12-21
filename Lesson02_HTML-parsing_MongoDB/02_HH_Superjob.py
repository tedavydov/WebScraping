from sys import argv
from bs4 import BeautifulSoup as bs
from datetime import datetime as dt
import re
import requests
import pandas as pd

# ========== ВВОД ПАРАМЕТРОВ =======
vacancy_name = 'инженер'
site_name = 'hh'
main_link = 'https://hh.ru'
page_count = None
page_max = 10
if len(argv) == 2:
    vacancy_name = argv[1]
elif len(argv) == 3:
    vacancy_name = argv[1]
    if argv[2] == 'hh' or argv[2] == 'sj':
        site_name = argv[2]
    else:
        print(f'argv ERROR: ({argv[2]}) - допустимые параметры "hh" или "sj"')
elif len(argv) >= 4:
    vacancy_name = argv[1]
    if argv[2] == 'hh' or argv[2] == 'sj':
        site_name = argv[2]
    else:
        print(f'argv ERROR: ({argv[2]}) - допустимые параметры "hh" или "sj"')
    try:
        page_count = int(argv[3])
    except:
        print(f'argv ERROR: ({argv[3]}) - параметр должен быть целым числом')
else:
    print('Вы можете передать в аргументах скрипта (разделяя пробелом):\n'
          '  ключевое слово поиска вакансии (в этой версии скрипта - только одно слово),\n'
          '  код сайта ("hh" или "sj"),\n'
          '  число страниц для поиска (целое число) ..\n'
          'по умолчанию применяются параметры: инженер hh (и максимально возможное для сайта число страниц)')
    inp_param = input("Введите параметры через пробел").split()
    if inp_param and inp_param[0]:
        if len(inp_param) == 1:
            vacancy_name = inp_param[0]
        elif len(inp_param) == 2:
            vacancy_name = inp_param[0]
            if inp_param[1] == 'hh' or inp_param[1] == 'sj':
                site_name = inp_param[1]
            else:
                print(f'argv ERROR: ({inp_param[1]}) - допустимые параметры "hh" или "sj"')
        elif len(inp_param) == 3:
            vacancy_name = inp_param[0]
            if inp_param[1] == 'hh' or inp_param[1] == 'sj':
                site_name = inp_param[1]
            else:
                print(f'argv ERROR: ({inp_param[1]}) - допустимые параметры "hh" или "sj"')
            try:
                page_count = int(inp_param[2])
            except:
                print(f'argv ERROR: ({inp_param[2]}) - параметр должен быть целым числом')
# ====================================================================================
# после ввода параметров скрипта :
# ==== НАБОР ПАРАМЕТРОВ для HH ========
if site_name == 'hh':
    main_link = 'https://hh.ru'
    link = f'{main_link}/search/vacancy'
    page_max = 40
    params = {'L_save_area': 'true',
              'clusters': 'true',
              'enable_snippets': 'true',
              'text': vacancy_name,
              'showClusters': 'false'}
    search_tags = {'main': ['div', {'class': 'HH-VacancySidebarTrigger-Vacancy'}],
                   'next': ['a', {'class': 'HH-Pager-Controls-Next'}, 0, 'href'],
                   'tags': [
                       [['a', {'class': 'HH-LinkModifier'}],
                        {'name': 'text', 'link': 'href', 'id': 'data-vacancy-id'}],
                       [['span', {'data-qa': "vacancy-serp__vacancy-compensation"}], {'compensation': 'text'}],
                       [['a', {'data-qa': "vacancy-serp__vacancy-employer"}], {'employer': 'text'}],
                       [['span', {'data-qa': "vacancy-serp__vacancy-address"}], {'address': 'text'}]
                   ]}
# ==== НАБОР ПАРАМЕТРОВ для Superjob ========
elif site_name == 'sj':
    main_link = 'https://superjob.ru'
    link = f'{main_link}/vacancy/search/'
    page_max = 37
    params = {'keywords': vacancy_name}
    search_tags = {'main': ['div', {'class': 'jNMYr GPKTZ _1tH7S'}, 2],
                   'next': ['a', {'class': 'f-test-button-dalshe'}, 0, 'href'],
                   'tags': [
                       [['a', {'class': '_6AfZ9'}], {'name': 'text', 'link': 'href', 'id': 'html_8'}],
                       [['span', {'class': "_3mfro _2Wp8I PlM3e _2JVkc _2VHxz"}], {'compensation': 'text_html'}],
                       [['a', {'class': "_205Zx"}], {'employer': 'text'}],
                       [['span', {'class': "f-test-text-company-item-location"}], {'address': 'text_html'}]
                   ]}
# else:  # заглушка на случай добавления сайтов в дальнейшем
#     main_link = 'https://hh.ru'
# ==== НАБОРЫ ПАРАМЕТРОВ ===================================

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:84.0) Gecko/20100101 Firefox/84.0'}


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
        if not (zp.get('min')): zp['min'] = None
        if not (zp.get('max')): zp['max'] = None
        if zp.get('val'):
            zp['val'] = re.sub('[-\s.,—]*', '', zp.get('val'), count=0)
        else:
            zp['val'] = None
    else:
        zp = {'min': None, 'max': None, 'val': None}
    return zp


def go_to_link(dfm, lnk, par, head, tags):
    vacancy_finds = {}
    if isinstance(dfm, pd.DataFrame) and isinstance(tags, dict):
        page_next_url = None
        response = requests.get(lnk, params=par, headers=head)
        soup = bs(response.text, 'html.parser')

        if response.ok:
            main = tags.get('main')
            tag_list = tags.get('tags')
            next_param = tags.get('next')
            vacancies = soup.findAll(main[0], main[1])
            # =====================================
            id_list = vacancy_finds.get('id')
            if id_list:
                id_max = len(vacancy_finds.get('id')) - 1
            else:
                id_max = 0
                id_list = []
            id_flag = False
            # =====================================
            i = 1  # нумерация вакансий на странице
            # =====================================
            for vacancy in vacancies:
                if len(main) >= 3:
                    for x in range(main[2]):
                        vacancy = vacancy.parent
                for tg in tag_list:
                    tmp = vacancy.find(tg[0][0], tg[0][1])
                    for k in tg[1].keys():
                        if k == 'id':
                            id_flag = True
                        vacancy_tmp = vacancy_finds.get(k)
                        if not vacancy_tmp:
                            vacancy_tmp = []
                        if tmp:  # найден текущий tag
                            get_k = tg[1].get(k)
                            if get_k == 'text':
                                kx = tmp.text
                            elif get_k == 'text_html':
                                kx = tmp.text
                                kx = re.sub('\<.*?\>', ' ', kx, count=0)
                                kx = re.sub('&nbsp;', ' ', kx, count=0)
                                if k == 'address':
                                    kx = kx.split('•')[1].split(',')[0].strip()
                                if kx == 'По договорённости':
                                    kx = None
                            elif get_k == 'main_href' and k == 'link':
                                kx = main_link + tmp['href']
                            elif get_k == 'html_8' and k == 'id':
                                kx = tmp['href']
                                tend = kx.find('.html')
                                kx = kx[tend - 8:tend]
                            else:
                                kx = tmp[get_k]
                            # =====================
                            if k == 'compensation':  # tag зарплаты
                                vacancy_tmp.append(compensation_to_int(kx))
                            else:  # остальные tag - параметры вакансии
                                vacancy_tmp.append(kx)
                        else:  # НЕ найден текущий tag
                            # =====================
                            if k == 'compensation':  # tag зарплаты
                                vacancy_tmp.append({'min': None, 'max': None, 'val': None})
                            else:  # остальные tag - параметры вакансии
                                vacancy_tmp.append(None)
                        vacancy_finds[k] = vacancy_tmp
                # ==================================
                if not id_flag:
                    id_list.append(id_max + i)
                i += 1  # номер вакансии на странице
            # ======================================================================
            if not id_flag:
                vacancy_finds['id'] = id_list
            # ======================================================================
            f_dfm = pd.DataFrame(vacancy_finds).set_index('id')
            f_dfm.index.name = '_id'
            vac_dfm = dfm.append(f_dfm, sort=False)
            # ======================================================================
            page_next = soup.findAll(next_param[0], next_param[1])
            next_count = len(next_param)
            if page_next:
                if next_count == 2:
                    page_next_url = main_link + page_next
                elif next_count == 3:
                    page_next_url = main_link + page_next[next_param[2]]
                elif next_count >= 4:
                    page_next_url = main_link + page_next[next_param[2]][next_param[3]]
                else:
                    print(f'go_to_link ERROR : page_next - wrong number of parameters ({next_count})')
            return page_next_url, vac_dfm
    else:
        print(f'!!! ===== go_to_link ERROR : ===========\n'
              f'dfm - not is pd.DataFrame: {dfm}\n'
              f'or tags not is dict: {tags}')


if page_count:
    page_stop = min(page_max, page_count)
else:
    # page_stop = 5
    page_stop = page_max
page = 0

vacancies_dic = {'id': [], 'name': [], 'compensation': [],
                 'employer': [], 'address': [], 'link': []}
vacancies_df = pd.DataFrame(vacancies_dic).set_index('id')
vacancies_df.index.name = '_id'
while link and (page < page_stop):
    print(link)
    rez = go_to_link(vacancies_df, link, params, headers, search_tags)
    vacancies_df = rez[1]
    link = rez[0]
    page += 1

vacancies_df.to_csv(f'vacancies_{vacancy_name}_{site_name}_{dt.now().date()}_{dt.now().timestamp()}.csv')

print('=== vacancies_df ', '=' * 30, '=\n', vacancies_df)
