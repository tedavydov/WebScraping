from pprint import pprint
from pymongo import MongoClient

client = MongoClient('localhost', 27017)

# mongo_b = client.social_20210111
# collection = mongo_b['inst']

mongo_base = client.social_20210113
collection = mongo_base['inst']

# for xx in collection.find({'_id': '21138692365'}):
# for xx in collection.find({}):
#     print('-' * 100)
#     pprint(xx)

# найдем всех пользователей подписанных на пользователя с id = '6420450488'
# for followed in collection.find({'subscribed_all': '6420450488'}):
#     print('-' * 100)
#     pprint(followed)

# # найдем всех пользователей подписанных на пользователя с id = '21138692365'
# for followed in collection.find({'subscribed_all': '21138692365'}):
#     print('-' * 100)
#     pprint(followed)


# найдем в БД всех пользователей на которых подписан пользователь с id = '21138692365'
# найдем пользователя в БД
us_id = '21138692365'
for us in collection.find({'_id': us_id}):
    # пользователи - на которых подписан пользователь
    list_foll_user = us.get('subscribed_all')
    print('=' * 100, f'\nПОДПИСКИ для {us_id}:\n{list_foll_user}')
    # найдем в БД все подписки
    for foll in list_foll_user:
        for foll_user in collection.find({'_id': foll}):
            print('-' * 100)
            pprint(foll_user)
