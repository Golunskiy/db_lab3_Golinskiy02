import psycopg2
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


username = 'postgres'
password = '21132113'
database = 'sholop01_DB'
host = 'localhost'
port = '5432'

query_1 = '''
CREATE VIEW Number_Of_Unicorn AS
SELECT TRIM(place_country) AS place_country, COUNT(com_name) AS com_quantity FROM company, place
WHERE company.place_id = place.place_id
GROUP BY place_country
'''
query_2 = '''
CREATE VIEW Sum_Valuation AS
SELECT TRIM(place_country), SUM(CAST(SUBSTRING(com_valuation, 2, 10) AS FLOAT))
FROM company, place
WHERE company.place_id = place.place_id
GROUP BY place_country
'''
query_3 = '''
CREATE VIEW New_Company_Every_Year AS
SELECT EXTRACT(YEAR FROM com_joined) AS com_year, COUNT(com_joined) AS com_quantity 
FROM company
GROUP BY EXTRACT(YEAR FROM com_joined)
ORDER BY EXTRACT(YEAR FROM com_joined)
'''

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

with conn:
    cur1 = conn.cursor()
    cur1.execute('DROP VIEW IF EXISTS Number_Of_Unicorn')
    cur1.execute(query_1)
    cur1.execute('SELECT * FROM Number_Of_Unicorn')
    name_country = []
    company_quantity = []

    for row in cur1:
        name_country.append(row[0])
        company_quantity.append(row[1])

    figure, (bar_ax, pie_ax, bar_bx) = plt.subplots(1, 3, figsize=(12, 8))

    bar_ax.bar(name_country, company_quantity)
    bar_ax.set_title('Як багато компаній єдинорогів в певній країні', size=15)
    bar_ax.set_ylabel('Кількість', size=12)
    bar_ax.tick_params(axis='x' ,labelrotation=90, labelsize=8)
    bar_ax.yaxis.set_major_locator(ticker.MultipleLocator(100))


    cur2 = conn.cursor()
    cur2.execute('DROP VIEW IF EXISTS Sum_Valuation')
    cur2.execute(query_2)
    cur2.execute('SELECT * FROM Sum_Valuation')
    name_country = []
    company_valuation = []

    for row in cur2:
        name_country.append(row[0])
        company_valuation.append(row[1])

    name_country_10 = []
    company_valuation_10 = []

    for i in range(len(name_country)):
        if len(name_country_10) < 10:                             # додаємо перші 10 випадкових компаній
            name_country_10.append(name_country[i])
            company_valuation_10.append((company_valuation[i]))
        else:
            if company_valuation[i] > min(company_valuation_10):  # компанія має більшу ціну, ніж найдешевша з списку топ10
                index_min = company_valuation_10.index(min(company_valuation_10))   # дізнаємось індекс компанії, що треба замінити
                name_country_10[index_min] = name_country[i]
                company_valuation_10[index_min] = company_valuation[i]

    pie_ax.pie(company_valuation_10, labels=name_country_10, autopct='%1.1f%%', textprops={'fontsize': 10},
               rotatelabels=True, )
    pie_ax.set_title('Топ 10 сум найдорожчих компаній по країнам', size=15)


    cur3 = conn.cursor()
    cur3.execute('DROP VIEW IF EXISTS New_Company_Every_Year')
    cur3.execute(query_3)
    cur3.execute('SELECT * FROM New_Company_Every_Year')
    company_year = []
    company_quantity = []

    for row in cur3:
        company_year.append(int(row[0]))
        company_quantity.append(row[1])

    bar_bx.plot(company_year, company_quantity, marker='o')
    bar_bx.yaxis.set_major_locator(ticker.MultipleLocator(100))
    bar_bx.xaxis.set_major_locator(ticker.MultipleLocator(1))
    bar_bx.set_title('Кількість нових компаній в певний рік', size=15)
    bar_bx.set_ylabel('Кількість', size=12)
    bar_bx.set_xlabel('Рік', size=12)
    bar_bx.tick_params(axis='x', labelrotation=90)

figure.tight_layout()
plt.show()