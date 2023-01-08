import psycopg2
import matplotlib.pyplot as plt
import csv
import pandas as pd
import numpy as np


username = 'postgres'
password = '21132113'
database = 'sholop01_DB'
host = 'localhost'
port = '5432'


query_0 = """
DELETE FROM Company;
DELETE FROM Place;
DELETE FROM Industry;
"""

query_place = """
INSERT INTO Place(place_id, place_city, place_country) VALUES ('%s', '%s', '%s')
"""

query_industry_1 = """
INSERT INTO Industry(ind_id, ind_1) VALUES ('%s', '%s')
"""
query_industry_2 = """
INSERT INTO Industry(ind_id, ind_1, ind_2) VALUES ('%s', '%s', '%s')
"""
query_industry_3 = """
INSERT INTO Industry(ind_id, ind_1, ind_2, ind_3) VALUES ('%s', '%s', '%s', '%s')
"""

query_company_ind1 = """
INSERT INTO Company(com_id, com_name, com_valuation, com_joined, place_id, ind_id) 
VALUES ('%s', '%s', '%s', '%s', 
(SELECT place_id FROM Place WHERE place_city = '%s'), 
(SELECT ind_id FROM Industry WHERE ind_1 = '%s'))
"""
query_company_ind2 = """
INSERT INTO Company(com_id, com_name, com_valuation, com_joined, place_id, ind_id) 
VALUES ('%s', '%s', '%s', '%s', 
(SELECT place_id FROM Place WHERE place_city = '%s'), 
(SELECT ind_id FROM Industry WHERE ind_1 = '%s' AND ind_2 = '%s'))
"""
query_company_ind3 = """
INSERT INTO Company(com_id, com_name, com_valuation, com_joined, place_id, ind_id) 
VALUES ('%s', '%s', '%s', '%s', 
(SELECT place_id FROM Place WHERE place_city = '%s'), 
(SELECT ind_id FROM Industry WHERE ind_1 = '%s' AND ind_2 = '%s' AND ind_3 = '%s'))
"""


data = pd.read_csv(r'unicorns till sep 2022.csv')

conn = psycopg2.connect(user=username, password=password, dbname=database)

with conn:
    cur = conn.cursor()
    cur.execute(query_0)

    cur1 = conn.cursor()
    df_city = pd.DataFrame(data, columns=['City '])     # беремо з таблиці дані
    df_country = pd.DataFrame(data, columns=['Country'])   # беремо з таблиці дані

    unique_city = []      # тут будуть унікальні значення міст
    unique_country = []   # тут будуть унікальні значення країн

    list_city = df_city.values.tolist()        # перетворюємо дані з таблиці в список
    list_country = df_country.values.tolist()  # перетворюємо дані з таблиці в список
    for i in range(len(list_city)):
        if list_city[i][0] not in unique_city:
            unique_city.append(*list_city[i])
            unique_country.append(*list_country[i])

    for i in range(len(unique_city)):
        without_apostrophe = unique_city[i].replace('\'', '')
        query = query_place % (i, without_apostrophe, unique_country[i])
        cur1.execute(query)
    conn.commit()



    cur2 = conn.cursor()
    df_industry = pd.DataFrame(data, columns=['Industry'])

    list_industry = df_industry.values.tolist()
    list_industry_new = []    # тут буде те саме, тільки елементи не будуть у вкладеному масиві

    # робимо наш список без лишніх деталів, щоб потім легко його розділити наокремі елементи
    for i in range(len(list_industry)):
        if ', &' in list_industry[i][0]:
            elem = list_industry[i][0].replace(', &', ',')
            list_industry_new.append(elem)
        elif '&' in list_industry[i][0]:
            elem = list_industry[i][0].replace(' &', ',')
            list_industry_new.append(elem)
        else:
            list_industry_new.append(list_industry[i][0])

    # розділюємо список
    for i in range(len(list_industry_new)):
        list_industry_new[i] = list_industry_new[i].split(', ')


    unique_industry = []

    # тільки унікальні значення
    for i in range(len(list_industry_new)):
        if list_industry_new[i] not in unique_industry:

            # видаляємо апострофи, вони ламають код
            without_apostrophe = []
            for j in range(len(list_industry_new[i])):
                without_apostrophe.append(list_industry_new[i][j].replace('\'', ''))

            unique_industry.append(without_apostrophe)


    for i in range(len(unique_industry)):
        if len(unique_industry[i]) == 1:
            query = query_industry_1 % (i, unique_industry[i][0])
        elif len(unique_industry[i]) == 2:
            query = query_industry_2 % (i, unique_industry[i][0], unique_industry[i][1])
        elif len(unique_industry[i]) == 3:
            query = query_industry_3 % (i, unique_industry[i][0], unique_industry[i][1], unique_industry[i][2])
        cur2.execute(query)
    conn.commit()


    cur3 = conn.cursor()
    df = pd.DataFrame(data, columns=['Company', 'Valuation ($B)', 'Date Joined', 'City ', 'Country'])
    com_name = df['Company'].tolist()
    com_valuation = df['Valuation ($B)'].tolist()
    com_joined_bad = df['Date Joined'].tolist()   # потрібно ще поміняти місяць і день місцями, оскільки sql їх некоректно відображає
    com_city = df['City '].tolist()
    com_country = df['Country'].tolist()
    # industry ми не беремо, бо вони у нас уже є і розкладені так як нам потрібно

    # робимо заміну місяця і дня в датах
    com_joined = []
    for i in range(len(com_joined_bad)):
        x = com_joined_bad[i].split('/')
        com_joined.append(x[1] + '/' + x[0] + '/' + x[2])

    # забираємо апострофи у назвах компаній
    for i in range(len(com_name)):
        com_name[i] = com_name[i].replace('\'', '')

    # забираємо апострофи у містах
    for i in range(len(com_city)):
        com_city[i] = com_city[i].replace('\'', '')

    # забираємо апострофи в індустріях
    for i in range(len(list_industry_new)):
        without_apostrophe = []
        for j in range(len(list_industry_new[i])):
            without_apostrophe.append(list_industry_new[i][j].replace('\'', ''))
        list_industry_new[i] = without_apostrophe


    for i in range(len(com_name)):
        if len(list_industry_new[i]) == 1:
            query = query_company_ind1 % (i, com_name[i], com_valuation[i], com_joined[i],
                                          com_city[i], list_industry_new[i][0])
        elif len(list_industry_new[i]) == 2:
            query = query_company_ind2 % (i, com_name[i], com_valuation[i], com_joined[i],
                                          com_city[i], list_industry_new[i][0], list_industry_new[i][1])
        elif len(list_industry_new[i]) == 3:
            query = query_company_ind3 % (i, com_name[i], com_valuation[i], com_joined[i],
                                          com_city[i], list_industry_new[i][0],
                                          list_industry_new[i][1], list_industry_new[i][2])
        cur3.execute(query)
    conn.commit()




    # eu_name = df['event'].tolist()
    # eu_city = df['host_city'].tolist()
    # eu_country = df['host_country'].tolist()
    #
    # unique_eu_name = []
    # unique_eu_city = []
    # unique_eu_country = []
    #
    # for i in range(len(eu_name)):
    #     if eu_name[i] not in unique_eu_name:
    #         unique_eu_name.append(eu_name[i])
    #         unique_eu_city.append(eu_city[i])
    #         unique_eu_country.append((eu_country[i]))
    #
    #
    #
    # for i in range(len(unique_eu_name)):
    #     query = query_eurovision % (i, unique_eu_name[i], unique_eu_city[i], unique_eu_country[i])
    #     cur2.execute(query)
    #
    # conn.commit()
    #
    # cur3 = conn.cursor()
    # df = pd.DataFrame(data, columns=['artist', 'artist_country', 'total_points', 'event'])
    # ar_artist = df['artist'].tolist()
    # ar_artist_country = df['artist_country'].tolist()
    # ar_total_points = df['total_points'].tolist()
    # ar_event = df['event'].tolist()
    #
    # unique_ar_artist = []
    # unique_ar_artist_country = []
    # unique_ar_total_points = []
    # unique_ar_event = []
    #
    # for i in range(len(ar_artist)):
    #     if ar_artist[i] not in unique_ar_artist:
    #         unique_ar_artist.append(ar_artist[i])
    #         unique_ar_artist_country.append(ar_artist_country[i])
    #         unique_ar_total_points.append(ar_total_points[i])
    #         unique_ar_event.append(ar_event[i])
    #
    # for i in range(len(unique_ar_artist)):
    #     query = query_artist % (i,
    #                             unique_ar_artist[i].replace("\'", ''),
    #                             unique_ar_artist_country[i],
    #                             0 if np.isnan(unique_ar_total_points[i]) else int(unique_ar_total_points[i]),
    #                             unique_ar_event[i])
    #     cur3.execute(query)
    #
    # conn.commit()
    #
    # cur4 = conn.cursor()
    # df = pd.DataFrame(data, columns=['song', 'artist', 'event'])
    # sg_song = df['song']
    # sg_artist = df['artist']
    # sg_event = df['event']
    #
    # for i in range(len(sg_song)):
    #     song_name = str(sg_song[i]).replace("\'", '')
    #     artist_name = sg_artist[i].replace("\'", '')
    #
    #     query = query_song % (i, song_name, artist_name)
    #     cur4.execute(query)
    #
    # conn.commit()
