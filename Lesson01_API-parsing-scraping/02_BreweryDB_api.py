import requests
from pprint import pformat, pprint

key = '3812442b7d4edbac495de5755a69d129'
main_link = f'http://api.brewerydb.com/v2/'
my_params = {
    'key': key
}

url = main_link + '/beers/'
response1 = requests.get(url, params=my_params)

if response1.ok:
    j_data1 = response1.json()
    try:
        with open('02_response_beers.json', "w", encoding="utf-8") as res_file:
            print(pformat(j_data1), file=res_file)
    except:
        print('Ошибка записи файла')

    print("=" * 10, f"Каталог сортов пива: (стр.{j_data1['currentPage']}): ", "=" * 25)
    i = 1
    for el in j_data1['data']:
        # print(f" сорт[{str(i)}]: {el['id']}")
        print(f" сорт[{i:02.0f}]: {el['name']}, "
              f"id: {el['id']}, "
              f"Содержание алкоголя: {el.get('abv')}, "
              f"Добавлено в каталог: {el['createDate']}")
        i += 1

# ПРИМЕР ВЫВОДА :
# Каталог сортов пива: (стр.1):
# сорт[01]: 'Murican Pilsner, id: c4f2KE, Содержание алкоголя: 5.5, Добавлено в каталог: 2013-08-19 11:58:12
# сорт[02]: 11.5° PLATO, id: zTTWa2, Содержание алкоголя: 4.5, Добавлено в каталог: 2016-08-09 14:44:42
# сорт[03]: 12th Of Never, id: zfP2fK, Содержание алкоголя: 5.5, Добавлено в каталог: 2016-08-03 23:25:54
# сорт[04]: 15th Anniversary Ale, id: xwYSL2, Содержание алкоголя: None, Добавлено в каталог: 2015-04-16 15:44:15
# сорт[05]: 16 So Fine Red Wheat Wine, id: UJGpVS, Содержание алкоголя: 11, Добавлено в каталог: 2013-02-24 16:31:05
# ..
# всего 50 сортов на странице

beer_id = 'MTLa3r'
url = main_link + f'/beer/{beer_id}/breweries'
response2 = requests.get(url, params=my_params)

if response2.ok:
    j_data2 = response2.json()
    try:
        with open(f'02_response_breweries_beer({beer_id}).json', "w", encoding="utf-8") as res_file2:
            print(pformat(j_data2), file=res_file2)
    except:
        print('Ошибка записи файла')

    print("=" * 10, f"Пример уточнения: где производят пиво (id: {beer_id}): ", "=" * 25)
    i = 1
    for el in j_data2['data']:
        print(f" сорт[{i:02.0f}]: {el['name']}, "
              f"id: {el['id']}, "
              f"Данные обновлены: {el['updateDate']}, "
              f"Добавлено в каталог: {el['createDate']}")
        i += 1
