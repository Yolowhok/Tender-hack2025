import pandas as pd
import sqlite3 

# data = pd.read_csv('data-set.csv', sep=';', encoding='utf-8')
# 3. Создаете подключение к базе данных SQLite:
# Чтение данных из CSV
df = pd.read_csv('data-set.csv', sep=";")

# Создание подключения к SQLite
conn = sqlite3.connect('tender.db')

# Запись данных в таблицу в SQLite (если таблица не существует, она будет создана)
df.to_sql('data', conn, if_exists='replace', index=False)

# Выполнение SQL-запроса
query_result = pd.read_sql_query('SELECT * FROM data', conn)


# result = pd.read_sql_query('SELECT * FROM data')
# Закрытие соединения
# conn.close()

# Просмотр результатов
print(query_result)

inn = 7721663977
# session_by_INN = pd.read_sql_query(f'SELECT * FROM data WHERE data."Участники КС - поставщики"  LIKE \'%{inn}%\'', conn)

# UNIC_SESSION_SQL = f'''SELECT DISTINCT data."Id КС"
# FROM data
# WHERE data."Участники КС - поставщики" LIKE '%{inn}%';'''



# API: GET /INNS

# UNIC_POSTAVSHIK = '''SELECT DISTINCT  data."Участники КС - поставщики" FROM data'''
#ДАТА ФРЕЙМ
# postavshik_df = pd.read_sql_query(UNIC_POSTAVSHIK, conn)

# df['ИНН'] = postavshik_df['Информация'].str.extract(r'ИНН:(\d{10})')

# conn.close


#ГРАФИК ПОЛУЧЕНИЯ ПОБЕД В РАЗРЕЗЕ ДАТА И КОНЕЧНАЯ ЦЕНА
GET_WINNS_COST_AND_DATA = f'''SELECT DISTINCT data."id КС", data."Конечная цена КС (победителя в КС)", data."Окончание КС"
from data
WHERE data."ИНН победителя КС" = \'{inn}\''''
postavshik_df = pd.read_sql_query(GET_WINNS_COST_AND_DATA, conn)
# print(postavshik_df)



#ГРАФИК ПОЛУЧЕНИЯ ПОБЕД В РАЗРЕЗЕ ДАТА И КОНЕЧНАЯ ЦЕНА С ВРЕМЕННЫМ ИНТЕРВАЛОМ
start_date = "2023-01-01"
end_date = "2023-12-31"
inn = "7721663977"
GET_WINS_COST_AND_DATA_BETWEEN = f'''SELECT DISTINCT data."id КС", data."Конечная цена КС (победителя в КС)", data."Окончание КС"
FROM data
WHERE data."ИНН победителя КС" = \'{inn}\'
AND data."Окончание КС" BETWEEN \'{start_date}\' AND \'{end_date}\''''



dt = pd.read_sql_query(GET_WINS_COST_AND_DATA_BETWEEN, conn)


#ПОЛУЧИТЬ УНИКАЛЬНЫЕ ЗНАЧЕНИЯ КПГЗ
SQL_GET_UNIC_KPGZ = '''SELECT DISTINCT(data."Код КПГЗ"), data."Наименование КПГЗ" from data
	Order by 1'''
dt = pd.read_sql_query(SQL_GET_UNIC_KPGZ, conn)

#ПОЛУЧИТЬ УНИКАЛЬНЫЕ ЗНАЧЕНИЯ ПО ОБЛАСТЯМ
SQL_GET_UNIC_CITY = '''SELECT DISTINCT(data."Регион победителя КС") from data Order by 1'''
dt = pd.read_sql_query(SQL_GET_UNIC_CITY, conn)




code_kpgz = '\'01.02.11.04.14.02\''
code_kpgz = 'null'

answer = ''
result = ''
if (answer == 'null'):
    result = 'null'
else:
    result = f'\'{answer}\''



code_kpgz = '\'01.02.11.04%\''
# city = '\'Москва\''
# city = '\'Санкт-Петербург г\''
city = 'null'
SQL_GET_UNICK_KC_WITH_PARAMS = f'''SELECT data."Код КПГЗ", data."Регион победителя КС" FROM data WHERE (data."Код КПГЗ" LIKE {code_kpgz} OR {code_kpgz} IS NULL) AND (data."Регион победителя КС" = {city} OR {city} IS NULL)'''

dt = pd.read_sql_query(SQL_GET_UNICK_KC_WITH_PARAMS, conn)
print(dt)
# unic_kc = pd.read_sql_query(UNIC_SESSION_SQL, conn)

# print(unic_kc)
# print(session_by_INN)
     






