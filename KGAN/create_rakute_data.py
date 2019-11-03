import pandas as pd
from collections import defaultdict
from tqdm import tqdm
import os
import numpy as np

data_path = './data/rakuten/'
data_files = [
                'ichiba04_review201003_20140221.tsv',
                'ichiba04_review201004_20140221.tsv',
                'ichiba04_review201005_20140221.tsv',
                'ichiba04_review201006_20140221.tsv',
                'ichiba04_review201007_20140221.tsv',
                'ichiba04_review201008_20140221.tsv',
                'ichiba04_review201009_20140221.tsv',
                'ichiba04_review201010_20140221.tsv',
                'ichiba04_review201011_20140221.tsv',
                'ichiba04_review201012_20140221.tsv',
             ]

NOT_VALID_USER = '購入者さん'

'''
data
*******************************
0 : user352346
1 : 20
2 : 1
3 : kumorizora:763825
4 : 煮出せば2L　お茶パック 10個入「オーガニック・ルイボスティー」レッド版
5 : 紅茶屋くもりぞら
6 : kumorizora/763825/
7 : 201505
8 : 420
9 : 1
10 : 実用品・普段使い
11 : 自分用
12 : リピート
13 : 5
14 : 
15 : 水だし用ルイボスティーバックてなかなか売っていないのでいつもここで購入しています。 麦茶は体を冷やすことを知って以来、冷蔵庫に常備しているのはルイボスティー。 飲みやすく、色がきれいでお勧めです。
16 : 2010-03-01 00:00:03
*******************************
'''

file_counter = 0


'''
*************************************************
read files
'''
print('Loading Files ...')

for file_name in data_files:

    data_list_0  = []
    data_list_1  = []
    data_list_2  = []
    data_list_3  = []
    data_list_4  = []
    data_list_5  = []
    data_list_6  = []
    data_list_7  = []
    data_list_8  = []
    data_list_9  = []
    data_list_10 = []
    data_list_11 = []
    data_list_12 = []
    data_list_13 = []
    data_list_14 = []
    data_list_15 = []
    data_list_16 = []


    f = open(data_path + file_name)
    line = f.readline()

    conunter = 0

    # while line and conunter < 100: # for test 
    while line:
        data = line.strip()
        data_list = data.split('\t')

        if data_list[0] != NOT_VALID_USER:
            data_list_0  .append(data_list[0])
            data_list_1  .append(data_list[1])
            data_list_2  .append(data_list[2])
            data_list_3  .append(data_list[3])
            data_list_4  .append(data_list[4])
            data_list_5  .append(data_list[5])
            data_list_6  .append(data_list[6])
            data_list_7  .append(data_list[7])
            data_list_8  .append(data_list[8])
            data_list_9  .append(data_list[9])
            data_list_10 .append(data_list[10])
            data_list_11 .append(data_list[11])
            data_list_12 .append(data_list[12])
            data_list_13 .append(data_list[13])
            data_list_14 .append(data_list[14])
            data_list_15 .append(data_list[15])
            data_list_16 .append(data_list[16])

        conunter += 1
        line = f.readline()

    data_df = pd.DataFrame(
        data = {
            '0'  : data_list_0,     # user_id　★
            '1'  : data_list_1,     # ?
            '2'  : data_list_2,     # ?
            '3'  : data_list_3,     # item_id ★
            '4'  : data_list_4,     # item_name
            '5'  : data_list_5,     # store_name
            '6'  : data_list_6,     # URL
            '7'  : data_list_7,     # store_id　★
            '8'  : data_list_8,     # price
            '9'  : data_list_9,     # ?
            '10' : data_list_10,    # 使用用途（実用品・普段使い ...）
            '11' : data_list_11,    # buy_for (家族へ・自分へ ...)
            '12' : data_list_12,    # frequency (はじめて・リピート)
            '13' : data_list_13,    # score
            '14' : data_list_14,    # review_title
            '15' : data_list_15,    # review
            '16' : data_list_16,    # date
        },
        columns = ['0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16']
    )

    if file_counter == 0 : 
        all_data_df = data_df
    else:
        all_data_df = pd.concat([all_data_df, data_df])
    
    file_counter += 1

user_id_list  = list(data_df['0'])
item_id_list  = list(data_df['3'])
store_id_list = list(data_df['7'])

df = pd.DataFrame(
    data = {
        'user_id'  : user_id_list,
        'item_id'  : item_id_list,
        'store_id' : store_id_list   
    },
    columns = ['user_id', 'item_id', 'store_id']
)

print('loading all data done ...!')

user_unique_list  = set(user_id_list)
item_unique_list  = set(item_id_list)
store_unique_list = set(store_id_list)

entity_unique_list = set(item_unique_list + store_unique_list)

print('=== DATA ANALYSIS ===\n')
print('user num : {}'.format(len(user_unique_list)))
print('item num : {}'.format(len(item_unique_list)))
print('store num : {}'.format(len(store_unique_list)))
print('intaraction : {}'.format(len(item_id_list)))
print('\n=====================')

user_id_map   = defaultdict(int)
item_id_map   = defaultdict(int)
entity_id_map = defaultdict(int)

user_counter = 0
for u in user_unique_list:
    user_id_map[u] = user_counter
    user_counter += 1

item_counter = 0
for i in item_unique_list:
    item_id_map[i] = item_counter
    item_counter += 1


entity_counter = 0
for e in entity_unique_list:
    entity_id_map[e] = entity_counter
    entity_counter += 1

def map_org_user_id(org_user_id):
    return user_id_map[org_user_id]

def map_org_entity_id(org_entity_id):
    return entity_id_map[org_entity_id]

df['user_id']  = df['user_id'].map(map_org_user_id)
df['item_id']  = df['item_id'].map(map_org_entity_id)
df['store_id'] = df['store_id'].map(map_org_entity_id)