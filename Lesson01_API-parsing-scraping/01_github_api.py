import requests
from pprint import pformat

user = 'netbox-community'
main_link = f'https://api.github.com/users/{user}/repos'
short_params = {
    'sort': 'full_name'
}
response = requests.get(main_link, params=short_params)

if response.ok:
    j_data = response.json()
    try:
        with open('01_response.json', "w", encoding="utf-8") as res_file:
            print(pformat(j_data), file=res_file)
    except:
        print('Ошибка записи файла')

    print(f"У пользователя {user} найдены репозитории :")
    for el in j_data:
        print(f" id: {el['id']}, name: {el['name']}")
