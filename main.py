from fastapi import FastAPI, Query
import pandas as pd
import sqlite3
from fastapi.responses import HTMLResponse
from starlette.requests import Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from datetime import datetime
import matplotlib.pyplot as plt
from fastapi.responses import StreamingResponse
import io
from matplotlib.ticker import ScalarFormatter
import matplotlib.ticker as mticker






app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount("/templates", StaticFiles(directory="templates"), name="templates")
conn = sqlite3.connect('tender.db', check_same_thread=False)

@app.get("/hello", response_class=HTMLResponse)
def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "name": "Мир"})



@app.get('/oldcustomers')
def get_start_customers():
    
    UNIC_CITY = pd.read_sql_query('''SELECT data."Регион победителя КС"  FROM data GROUP BY data."Регион победителя КС"''', conn)

    UNIC_KPGZ = pd.read_sql_query('''SELECT data."Код КПГЗ"  FROM data GROUP BY data."Код КПГЗ"''', conn)

    UNIC_CUSTOMERS = pd.read_sql_query('''SELECT data."ИНН заказчика", data."Наименование заказчика"  FROM data GROUP BY data."ИНН заказчика"''', conn)

    kpgz_code_UPD= 'null'
    winner_region_UPD= 'null'
    start_date_UPD= 'null'
    end_date_UPD= 'null'
    inn_UPD = 'null'

    first100 = pd.read_sql_query(f'''SELECT data."Id КС", data.*  FROM data WHERE 
    (data."Код КПГЗ" = {kpgz_code_UPD} OR {kpgz_code_UPD} IS NULL) AND
    (data."Регион победителя КС" = {winner_region_UPD} OR {winner_region_UPD} IS NULL) AND
    (data."Окончание КС" BETWEEN {start_date_UPD} AND {end_date_UPD} OR ({start_date_UPD} IS NULL AND {end_date_UPD} IS NULL)) AND
    (data."ИНН победителя КС" = {inn_UPD} OR {inn_UPD} IS NULL) GROUP BY data."Id КС" lIMIT 100''', conn)



    return {"city": UNIC_KPGZ.to_dict(orient='records'),
            "kpgz:": UNIC_CITY.to_dict(orient='records'),
            "customers": UNIC_CUSTOMERS.to_dict(orient='records'),
            "first100": first100.to_dict(orient='records')}
    



@app.get("/customers/config/")
def get_tenders(request: Request,
    kpgz_code: str = Query(None),
    winner_region: str = Query(None),
    start_date: str = Query(None),
    end_date: str = Query(None),
    inn: str = Query(None),
    do: str = Query(None),
    min_price: str = Query(None),
    max_price: str = Query(None),
    customers: str = Query(None),
    win: str = Query(None)
):

    # Подключаемся к базе данных
    conn = sqlite3.connect('tender.db')
    kpgz_code_UPD = '',
    winner_region_UPD =  ''
    start_date_UPD = ''
    end_date_UPD = ''
    inn_UPD = ''
    min_price_upd = ''
    max_price_upd = ''
    inn_variant_UPD = ''
    do_upd = ''
    concatQUERY = ''
    customers_upd = ''
    win_upd = ''
     
    kpgz_code_UPD = 'null' if  kpgz_code == None else f'\'{kpgz_code}%\''
    winner_region_UPD = 'null' if  winner_region == None else f'\'{winner_region}\''
    start_date_UPD = 'null' if  start_date == None else f'\'{start_date}\''
    end_date_UPD = 'null' if  end_date == None else f'\'{end_date}\''
    min_price_upd = 'null' if  min_price == None else f'\'{min_price}\''
    max_price_upd = 'null' if  max_price == None else f'\'{max_price}\''
    customers_upd = 'null' if  customers == None else f'\'{customers}\''
    win_upd = 'null' if  win == None else f'\'{inn}\''
    do_upd = 'null' if  do == None else f'\'{do}\''


    # если выбрали только начало или конец даты
    if (end_date_UPD == 'null' and start_date_UPD != 'null'):
        # Получаем сегодняшнюю дату
        today = datetime.now()
        # Форматируем дату в нужный формат
        formatted_date = today.strftime("%Y-%m-%d")
        end_date_UPD = f'''\'{formatted_date}\''''
    if (start_date_UPD == 'null' and end_date_UPD != 'null'):
        start_date_UPD = '\'1900-01-01\''
    # print(start_date_UPD)
    # print(end_date_UPD)


    # query = f'''SELECT 
    #     DISTINCT data."Id КС", 
    # CAST(TRIM(REPLACE(REPLACE(trim(data."Конечная цена КС (победителя в КС)"), '\t', ''), CHAR(160), '')) AS DECIMAL(10, 2)) AS "Конечная цена КС (победителя в КС)",
    # CAST(TRIM(REPLACE(REPLACE(trim(data."Начальная цена КС"), '\t' , ''), CHAR(160), '')) AS DECIMAL(10, 2)) AS "Начальная цена КС",

    # data."Ссылка на КС", data."ИНН заказчика", data."Наименование заказчика", data."Регион заказчика", 
    # data."Начало КС", data."Окончание КС", data."ИНН победителя КС", data."Наименование победителя КС", data."Регион победителя КС",
    # data."Участники КС - поставщики", data."Код КПГЗ", data."Наименование КПГЗ", 
    #     CASE 
    #        WHEN "Участники КС - поставщики" LIKE '%{inn}%' THEN 'true'
    #        ELSE 'false'
    #     END AS "do" 
    #     FROM data 
    #         WHERE 
    #             (data."Код КПГЗ" LIKE {kpgz_code_UPD} OR {kpgz_code_UPD} IS NULL) AND
    #             (data."Регион победителя КС" = {winner_region_UPD} OR {winner_region_UPD} IS NULL) AND
    #             (data."Окончание КС" BETWEEN {start_date_UPD} AND {end_date_UPD} OR ({start_date_UPD} IS NULL AND {end_date_UPD} IS NULL)) AND 
    #             (data."Регион победителя КС" = {winner_region_UPD} OR {winner_region_UPD} IS NULL) AND
    #             (data."ИНН победителя КС" = {win_upd} OR {win_upd} IS NULL) AND
    #             (CAST(TRIM(REPLACE(REPLACE(data."Конечная цена КС (победителя в КС)", CHAR(9), ''), CHAR(160), '')) AS FLOAT) > {min_price_upd} OR {min_price_upd} IS NULL) AND 
    #             (CAST(TRIM(REPLACE(REPLACE(data."Конечная цена КС (победителя в КС)", CHAR(9), ''), CHAR(160), '')) AS FLOAT) < {max_price_upd} OR {max_price_upd} IS NULL) AND
    #             (data."ИНН заказчика" = {customers} OR {customers} IS NULL) AND
    #             (do = {do_upd} OR {do_upd} IS NULL) 

    #     GROUP BY data."Id КС"'''   
    query = f'''SELECT 
        DISTINCT data."Id КС", 
    CAST(TRIM(REPLACE(REPLACE(trim(data."Конечная цена КС (победителя в КС)"), '\t', ''), CHAR(160), '')) AS DECIMAL(10, 2)) AS "Конечная цена КС (победителя в КС)",
    CAST(TRIM(REPLACE(REPLACE(trim(data."Начальная цена КС"), '\t' , ''), CHAR(160), '')) AS DECIMAL(10, 2)) AS "Начальная цена КС",

    data."Ссылка на КС", data."ИНН заказчика", data."Наименование заказчика", data."Регион заказчика", 
    data."Начало КС", data."Окончание КС", data."ИНН победителя КС", data."Наименование победителя КС", data."Регион победителя КС",
    data."Участники КС - поставщики", data."Код КПГЗ", data."Наименование КПГЗ", 
        CASE 
           WHEN "Участники КС - поставщики" LIKE '%{inn}%' THEN 'true'
           ELSE 'false'
        END AS "do" 
        FROM data 
            WHERE 
                (data."Код КПГЗ" LIKE {kpgz_code_UPD} OR {kpgz_code_UPD} IS NULL) AND
                (data."Регион победителя КС" = {winner_region_UPD} OR {winner_region_UPD} IS NULL) AND
                (data."Окончание КС" BETWEEN {start_date_UPD} AND {end_date_UPD} OR ({start_date_UPD} IS NULL AND {end_date_UPD} IS NULL)) AND 
                (data."ИНН победителя КС" = {win_upd} OR {win_upd} IS NULL) AND
                (CAST(TRIM(REPLACE(REPLACE(data."Конечная цена КС (победителя в КС)", CHAR(9), ''), CHAR(160), '')) AS FLOAT) > {min_price_upd} OR {min_price_upd} IS NULL) AND 
                (CAST(TRIM(REPLACE(REPLACE(data."Конечная цена КС (победителя в КС)", CHAR(9), ''), CHAR(160), '')) AS FLOAT) < {max_price_upd} OR {max_price_upd} IS NULL) AND
                (data."ИНН заказчика" = {customers_upd} OR {customers_upd} IS NULL) AND

                (do = {do_upd} OR {do_upd} IS NULL) 


        GROUP BY data."Id КС"
        '''   
    # query = f'''
    # SELECT * from data
    # limit 100
    #   '''   
    print(query)

    # params = [kpgz_code_UPD, winner_region_UPD, start_date_UPD, end_date_UPD, inn_UPD]
    # print(params)
    df = pd.read_sql_query(query, conn)


    print(df.columns)

    # df['diff'] = df['Конечная цена КС (победителя в КС)'] / df['Начальная цена КС']*100-100
    # df['sum_ystupki'] = df['Конечная цена КС (победителя в КС)'] - df['Начальная цена КС']
    # discount = abs(int (df['sum_ystupki'].sum()))
    df = df.rename(columns={'Id КС': 'id_ks',
                            'Конечная цена КС (победителя в КС)':'sum_end',
                            'Начальная цена КС':'sum_start',
                            'Ссылка на КС':'link',
                            'ИНН заказчика':'provider_inn',
                            'Наименование заказчика':'provider_org_name',
                            'Регион заказчика':'provider_region',
                            'Начало КС': 'start_date_ks',
                            'Окончание КС':'end_date_ks',
                            'ИНН победителя КС':'winner_inn',
                            'Наименование победителя КС':'winner_org',
                            'Регион победителя КС':'winner_city',
                            'Участники КС - поставщики':'members_providers',
                            'Код КПГЗ':'kpgz_code',
                            'Наименование КПГЗ': 'kpgz_name'})


    # print(discount)
    # total_sum = df['diff'].sum()/df['Id КС'].count()
    # total_sum = df['diff'].sum() / df['Id КС'].count()
    # total_sum_rounded = round(total_sum, 2)
    # print(total_sum)
    conn.close()





    # return {"KPI_DIFF_PROC": total_sum_rounded,
    #         "discount": discount,
    #         "data": df.to_dict(orient='records')}
    return{"data": df.to_dict(orient='records')}

@app.get("/sessions")
def get_sessions(request: Request):
    conn = sqlite3.connect('tender.db')
    query = '''
        SELECT data."Id КС", data."Наименование заказчика" FROM data 
	GROUP BY data."Id КС"
    '''
    session_df = pd.read_sql_query(query, conn)



    return {"data": session_df.to_dict(orient='records')}

@app.get("/customers")
def get_sessions(request: Request): 

    conn = sqlite3.connect('tender.db')
    query = '''
        SELECT data."ИНН заказчика", data."Наименование заказчика" FROM data 
	GROUP BY data."ИНН заказчика"
    '''
    customers_df = pd.read_sql_query(query, conn)



    return {"data": customers_df.to_dict(orient='records')}

@app.get("/kpgz")
def get_sessions(request: Request):
    conn = sqlite3.connect('tender.db')
    query = '''
        SELECT data."Код КПГЗ", data."Наименование КПГЗ" FROM data 
	GROUP BY data."Код КПГЗ"
    '''
    session_df = pd.read_sql_query(query, conn)



    return {"data": session_df.to_dict(orient='records')}

@app.get("/city")
def get_sessions(request: Request):

    conn = sqlite3.connect('tender.db')
    query = '''
        SELECT data."Регион заказчика" FROM data 
	GROUP BY data."Регион заказчика"
    '''
    session_df = pd.read_sql_query(query, conn)



    return {"data": session_df.to_dict(orient='records')}

@app.get("/provider")
def get_providers(request: Request):

    conn = sqlite3.connect('tender.db')
    query = '''
        SELECT data."Участники КС - поставщики" FROM data 
	        GROUP BY data."Id КС"
    '''
    providers_df = pd.read_sql_query(query, conn)

    # Объединяем все строки в один список участников
    all_participants = providers_df['Участники КС - поставщики'].str.cat(sep='; ')

    # Разделяем по '; ' и убираем лишние пробелы
    participants_list = [participant.strip() for participant in all_participants.split(';')]

    # print(participants_list)
    # print(type(participants_list))
    # print(participants_list[0])
    # print(participants_list[1])
    unique_participants_list = set(participants_list)

    unique_participants_dict = {}

    for item in unique_participants_list:
        # Разделяем ИНН и название организации
        try:
            inn, name = item.split(' ', 1)
            unique_participants_dict[inn] = name.strip()

        except ValueError:
            continue  # Если строка не в правильном формате, пропускаем




    # print(unique_participants_dict)
    # for key in unique_participants_dict:
    #     print(key, ":", unique_participants_dict[key])


    # Создание DataFrame





    # Создание DataFrame из словаря
    df = pd.DataFrame(list(unique_participants_dict.items()), columns=['inn', 'org'])
    df['inn'] = df['inn'].str.replace('ИНН:', '', case=False).str.strip()

    # Вывод DataFrame на экран
    # print(df)
        # print(df)



    # return {"data": unique_participants_dict}
    return {"data": df.to_dict(orient='records')}

@app.get('/plot/custom')
async def get_plots(request: Request,
    kpgz_code: str = Query(None),
    winner_region: str = Query(None),
    start_date: str = Query(None),
    end_date: str = Query(None),
    inn: str = Query(None),
    do: str = Query(None),
    min_price: str = Query(None),
    max_price: str = Query(None),
    customers: str = Query(None),
    win: str = Query(None)):

    # Подключаемся к базе данных
    conn = sqlite3.connect('tender.db')
    kpgz_code_UPD = '',
    winner_region_UPD =  ''
    start_date_UPD = ''
    end_date_UPD = ''
    inn_UPD = ''
    min_price_upd = ''
    max_price_upd = ''
    inn_variant_UPD = ''
    do_upd = ''
    concatQUERY = ''
    customers_upd = ''
    win_upd = ''
     
    kpgz_code_UPD = 'null' if  kpgz_code == None else f'\'{kpgz_code}%\''
    winner_region_UPD = 'null' if  winner_region == None else f'\'{winner_region}\''
    start_date_UPD = 'null' if  start_date == None else f'\'{start_date}\''
    end_date_UPD = 'null' if  end_date == None else f'\'{end_date}\''
    min_price_upd = 'null' if  min_price == None else f'\'{min_price}\''
    max_price_upd = 'null' if  max_price == None else f'\'{max_price}\''
    customers_upd = 'null' if  customers == None else f'\'{customers}\''
    win_upd = 'null' if  win == None else f'\'{inn}\''
    do_upd = 'null' if  do == None else f'\'{do}\''


    # если выбрали только начало или конец даты
    if (end_date_UPD == 'null' and start_date_UPD != 'null'):
        # Получаем сегодняшнюю дату
        today = datetime.now()
        # Форматируем дату в нужный формат
        formatted_date = today.strftime("%Y-%m-%d")
        end_date_UPD = f'''\'{formatted_date}\''''
    if (start_date_UPD == 'null' and end_date_UPD != 'null'):
        start_date_UPD = '\'1900-01-01\''

    query = f'''SELECT 
        DISTINCT data."Id КС", 
    CAST(TRIM(REPLACE(REPLACE(trim(data."Конечная цена КС (победителя в КС)"), '\t', ''), CHAR(160), '')) AS DECIMAL(10, 2)) AS "Конечная цена КС (победителя в КС)",
    CAST(TRIM(REPLACE(REPLACE(trim(data."Начальная цена КС"), '\t' , ''), CHAR(160), '')) AS DECIMAL(10, 2)) AS "Начальная цена КС",

    data."Ссылка на КС", data."ИНН заказчика", data."Наименование заказчика", data."Регион заказчика", 
    data."Начало КС", data."Окончание КС", data."ИНН победителя КС", data."Наименование победителя КС", data."Регион победителя КС",
    data."Участники КС - поставщики", data."Код КПГЗ", data."Наименование КПГЗ", 
        CASE 
           WHEN "Участники КС - поставщики" LIKE '%{inn}%' THEN 'true'
           ELSE 'false'
        END AS "do" 
        FROM data 
            WHERE 
                (data."Код КПГЗ" LIKE {kpgz_code_UPD} OR {kpgz_code_UPD} IS NULL) AND
                (data."Регион победителя КС" = {winner_region_UPD} OR {winner_region_UPD} IS NULL) AND
                (data."Окончание КС" BETWEEN {start_date_UPD} AND {end_date_UPD} OR ({start_date_UPD} IS NULL AND {end_date_UPD} IS NULL)) AND 
                (data."ИНН победителя КС" = {win_upd} OR {win_upd} IS NULL) AND
                (CAST(TRIM(REPLACE(REPLACE(data."Конечная цена КС (победителя в КС)", CHAR(9), ''), CHAR(160), '')) AS FLOAT) > {min_price_upd} OR {min_price_upd} IS NULL) AND 
                (CAST(TRIM(REPLACE(REPLACE(data."Конечная цена КС (победителя в КС)", CHAR(9), ''), CHAR(160), '')) AS FLOAT) < {max_price_upd} OR {max_price_upd} IS NULL) AND
                (data."ИНН заказчика" = {customers_upd} OR {customers_upd} IS NULL) AND

                (do = {do_upd} OR {do_upd} IS NULL) 


        GROUP BY data."Id КС"
        '''   

    print(query)

    df = pd.read_sql_query(query, conn)

    print(df.columns)

    df = df.rename(columns={'Id КС': 'id_ks',
                            'Конечная цена КС (победителя в КС)':'sum_end',
                            'Начальная цена КС':'sum_start',
                            'Ссылка на КС':'link',
                            'ИНН заказчика':'provider_inn',
                            'Наименование заказчика':'provider_org_name',
                            'Регион заказчика':'provider_region',
                            'Начало КС': 'start_date_ks',
                            'Окончание КС':'end_date_ks',
                            'ИНН победителя КС':'winner_inn',
                            'Наименование победителя КС':'winner_org',
                            'Регион победителя КС':'winner_city',
                            'Участники КС - поставщики':'members_providers',
                            'Код КПГЗ':'kpgz_code',
                            'Наименование КПГЗ': 'kpgz_name'})


    conn.close()




    # return{"data": df.to_dict(orient='records')}
    return get_plot(df['start_date_ks'], df['sum_end'])

# @app.get("/plot")
def get_plot(x, y):
    # Создаем график
    plt.figure(figsize=(10, 10))
    x = x.str.split(' ').str[0]


    plt.plot(x, y, marker='o')
    plt.title("Пример графика")
    plt.xlabel("Ось X")
    plt.ylabel("Ось Y")
    # Установка форматирования для оси Y
    ax = plt.gca()
    ax.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))  # Без научной нотации и с запятыми
    plt.xticks(rotation=45)

    # Уменьшение количества меток на оси X
    # if len(x) > 10:  # Например, если больше 10 меток, выбираем шаг
        # plt.xticks(ticks=range(0, len(x), max(1, len(x) // 10)))
    # if len(x) > 1000:  # Например, если больше 10 меток, выбираем шаг
        # plt.xticks(ticks=range(0, len(x), max(1, len(x) // 10)))

    


    # Сохраняем график в байтовый поток
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)

    plt.close()  # Закрываем график, освобождая память

    return StreamingResponse(buf, media_type="image/png")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
