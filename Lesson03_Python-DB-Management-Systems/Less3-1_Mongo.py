from sys import argv
import go_vacancies_soup as vacancies_new
import pandas as pd
from pymongo import MongoClient
from pprint import pprint

# ====================================================================================
vacancy_name = 'инженер'

# ========== ВВОД ПАРАМЕТРОВ ==================
if len(argv) >= 2 and isinstance(argv[1], int):
    if argv[1] <= 0:
        vacancies_new.start('hh')
        vacancies_new.start('sj')
    elif argv[1] == 1:
        vacancies_new.start('hh')
    elif argv[1] >= 2:
        vacancies_new.start('sj')
else:
    print('Вакансии будут загружены из ранее сохраненных файлов')


# ====================================================================================

# 1... реализовать функцию, записывающую собранные вакансии в созданную БД
def vacancy_to_db(vacancies, clt):
    count = vacancies.count(axis=0)[0]
    for x in range(count):
        vacancy = dict(vacancies.iloc[x])
        vacancy_id = int(vacancy['_id'])
        vacancy['_id'] = vacancy_id  # <class 'numpy.int64'> ==> <class 'int'>
        # проверка наличия вакансии с данным '_id' в БД
        if clt.count_documents({'_id': vacancy['_id']}) == 0:
            # запись новой вакансии в БД
            clt.insert_one(vacancy)
        # закомментировал == по условию задачи 3
        # 3. Написать функцию, которая будет добавлять в вашу базу данных
        #    ТОЛЬКО новые вакансии с сайта.
        # else:  # обновляет уже записанные в БД вакансии
        #     clt.replace_one({'_id': vacancy['_id']}, vacancy)


# ====================================================================================
# 2. Написать функцию, которая производит поиск и выводит на экран вакансии
#    с заработной платой больше введённой суммы.
# Поиск должен происходить по 2-ум полям (минимальной и максимальной зарплате)
def search_vacancy_max_salary(sal, clt):
    for vc in clt.find({'$or': [{'min': {'$gte': sal}}, {'max': {'$gte': sal}}]}):
        pprint(vc)


# ====================================================================================
# тест
# vacancies_new.start('hh', 3)
# vacancies_new.start('sj', 3)
# обновление вакансий
# vacancies_new.start('hh')
# vacancies_new.start('sj')
# ====================================================================================

vacancies_hh_df = pd.read_csv(f'vacancies_{vacancy_name}_hh.csv')
vacancies_sj_df = pd.read_csv(f'vacancies_{vacancy_name}_sj.csv')

client = MongoClient('127.0.0.1', 27017)
db = client['vacancies']

vacancy_to_db(vacancies_hh_df, db.hh)
vacancy_to_db(vacancies_sj_df, db.sj)

search_vacancy_max_salary(70000, db.hh)
search_vacancy_max_salary(70000, db.sj)

# для проверки записи в БД выведем вакансии с '_id' = 41065436 и 35160473
# for vc in db.hh.find({'_id': 41065436}):
#     pprint(vc)
# for vc in db.sj.find({'_id': 35160473}):
#     pprint(vc)

# вывод всех вакансий по HH из БД
# for vc in db.hh.find({}):
#     print(vc)
# вывод всех вакансий по SJ из БД
# for vc in db.sj.find({}):
#     print(vc)
