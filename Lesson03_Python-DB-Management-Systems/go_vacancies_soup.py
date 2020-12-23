from bs4 import BeautifulSoup as bs
import re
import requests
import pandas as pd
from datetime import datetime as dt


def start(site, pg=None, v_name=None):
    '''
    :param site: сайт поиска вакансий, допустимые значения "1"=="hh" или "2"=="sj"
    :param pg: page_count - параметр должен быть целым числом
    :param v_name: вакансия для поиска, будет использовано первое слово в строке
    :return:
    '''
    # ========== ВВОД ПАРАМЕТРОВ =======
    vacancy_name = 'инженер'
    site_name = 'hh'
    main_link = 'https://hh.ru'
    page_max = 10
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:84.0) Gecko/20100101 Firefox/84.0'}
    if isinstance(site, str):
        if site == 'hh' or site == 'sj':
            site_name = site
        else:
            print(f'start(site) ERROR: ({site}) - допустимые значения "hh" или "sj"')
    elif isinstance(site, int):
        if site == 1:
            site_name = 'hh'
        elif site == 2:
            site_name = 'sj'
        else:
            print(f'start(site) ERROR: ({site})'
                  f' - допустимые значения "1"=>"hh" или "2"=>"sj"')
    else:
        site_name = 'hh'
    # =======================
    if v_name:
        if isinstance(v_name, str):
            tmp_name = v_name.split()[0]
            if tmp_name:
                vacancy_name = tmp_name
        else:
            print(f'start(site, page_count, vacancy_name) ERROR: vacancy_name ({v_name})'
                  f' - параметр должен быть строкой, для поиска вакансии будет использовано первое слово в строке')
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
                           [['a', {'class': '_6AfZ9'}], {'name': 'text', 'link': 'main_href', 'id': 'html_8'}],
                           [['span', {'class': "_3mfro _2Wp8I PlM3e _2JVkc _2VHxz"}], {'compensation': 'text_html'}],
                           [['a', {'class': "_205Zx"}], {'employer': 'text'}],
                           [['span', {'class': "f-test-text-company-item-location"}], {'address': 'text_html'}]
                       ]}
    # else:  # заглушка на случай добавления сайтов в дальнейшем
    #     pass
    # ==== НАБОРЫ ПАРАМЕТРОВ ===================================
    # =========================================================
    if pg:
        if isinstance(pg, int):
            page_stop = min(page_max, pg)
        else:
            page_stop = page_max
            print(f'start(site, page_count) ERROR: page_count ({pg})'
                  f' - параметр должен быть целым числом')
    else:
        # page_stop = 5  # для тестов
        page_stop = page_max
    page = 0
    # =========================================================
    vacancies_dic = {'id': [], 'name': [], 'min': [], 'max': [], 'val': [],
                     'employer': [], 'address': [], 'link': []}
    vacancies_df = pd.DataFrame(vacancies_dic).set_index('id')
    vacancies_df.index.name = '_id'
    while link and (page < page_stop):
        print(link)
        rez = go_to_link(vacancies_df, link, main_link, params, headers, search_tags)
        vacancies_df = rez[1]
        link = rez[0]
        page += 1
    # =========================================================
    # vacancies_df.to_csv(f'vacancies_{vacancy_name}_{site_name}_{dt.now().date()}_{dt.now().timestamp()}.csv')
    vacancies_df.to_csv(f'vacancies_{vacancy_name}_{site_name}.csv')

    print(f'start({site}) ', '=' * 30, '=\n', vacancies_df)


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


def go_to_link(dfm, lnk, mlnk, par, head, tags):
    # vacancy_finds - словарь параметров(списков) по вакансиям на странице
    vacancy_finds = {'id': [], 'name': [], 'min': [], 'max': [], 'val': [],
                     'employer': [], 'address': [], 'link': []}
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
                for tg in tag_list:  # перебор правил для поиска параметров
                    tmp = vacancy.find(tg[0][0], tg[0][1])
                    for k in tg[1].keys():  # перебор параметров в правиле
                        if k == 'id':
                            id_flag = True
                        # =========================================================================
                        if k == 'compensation':  # tag зарплаты
                            k_tmp = {}
                            if vacancy_finds.get('min') \
                                    and vacancy_finds.get('max') and vacancy_finds.get('val'):
                                k_tmp['min'] = vacancy_finds.get('min')
                                k_tmp['max'] = vacancy_finds.get('max')
                                k_tmp['val'] = vacancy_finds.get('val')
                            else:
                                k_tmp['min'] = []
                                k_tmp['max'] = []
                                k_tmp['val'] = []
                        else:  # остальные tag - параметры вакансии
                            # k_tmp = список по вакансиям на странице для параметра (k)
                            k_tmp = vacancy_finds.get(k)  # загрузка списка по предыдущим вакансиям
                        if not k_tmp:
                            k_tmp = []
                        # =========================================================================
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
                                kx = mlnk + tmp['href']
                            elif get_k == 'html_8' and k == 'id':
                                kx = tmp['href']
                                tend = kx.find('.html')
                                kx = kx[tend - 8:tend]
                            else:
                                kx = tmp[get_k]
                            # =====================
                            if k == 'compensation':  # tag зарплаты
                                comp_tmp = compensation_to_int(kx)
                                k_tmp.get("min").append(comp_tmp.get("min"))
                                k_tmp.get('max').append(comp_tmp.get('max'))
                                k_tmp.get('val').append(comp_tmp.get('val'))
                            else:  # остальные tag - параметры вакансии
                                k_tmp.append(kx)
                        else:  # НЕ найден текущий tag
                            if k == 'compensation':  # tag зарплаты
                                k_tmp.get('min').append(None)
                                k_tmp.get('max').append(None)
                                k_tmp.get('val').append(None)
                            else:  # остальные tag - параметры вакансии
                                k_tmp.append(None)
                        # =========================================================================
                        # выгрузка параметра(списка) по вакансиям в словарь
                        if k == 'compensation':  # tag зарплаты
                            vacancy_finds['min'] = k_tmp.get('min')
                            vacancy_finds['max'] = k_tmp.get('max')
                            vacancy_finds['val'] = k_tmp.get('val')
                        else:  # остальные tag - параметры вакансии
                            vacancy_finds[k] = k_tmp
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
                    page_next_url = mlnk + page_next
                elif next_count == 3:
                    page_next_url = mlnk + page_next[next_param[2]]
                elif next_count >= 4:
                    page_next_url = mlnk + page_next[next_param[2]][next_param[3]]
                else:
                    print(f'go_to_link ERROR : page_next - wrong number of parameters ({next_count})')
            return page_next_url, vac_dfm
    else:
        print(f'!!! ===== go_to_link ERROR : ===========\n'
              f'dfm - not is pd.DataFrame: {dfm}\n'
              f'or tags not is dict: {tags}')
