#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta

import pendulum

date_now = datetime.datetime.now().date()

from functools import lru_cache

# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.generic_transfer import GenericTransfer
from airflow.providers.postgres.operators.postgres import PostgresOperator

# ________________________________________________________________________
# ________________________________________________________________________
# ________________________________________________________________________

import requests
import json

import cryptography

import pandas as pd
import numpy as np

import xmltodict

import re
import requests

import warnings
warnings.simplefilter("ignore")

from functools import lru_cache

import importlib

import modules.api_info
importlib.reload(modules.api_info)

from datetime import datetime

from sqlalchemy import create_engine

from modules.api_info import var_encrypt_var_app_client_id
from modules.api_info import var_encrypt_var_app_secret
from modules.api_info import var_encrypt_var_secret_key

from modules.api_info import var_encrypt_url_sbis
from modules.api_info import var_encrypt_url_sbis_unloading

from modules.api_info import var_encrypt_var_db_user_name
from modules.api_info import var_encrypt_var_db_user_pass

from modules.api_info import var_encrypt_var_db_host
from modules.api_info import var_encrypt_var_db_port

from modules.api_info import var_encrypt_var_db_name
from modules.api_info import var_encrypt_var_db_name_for_upl
from modules.api_info import var_encrypt_var_db_schema
from modules.api_info import var_encryptvar_API_sbis
from modules.api_info import var_encrypt_API_sbis_pass

from modules.api_info import f_decrypt, load_key_external


var_app_client_id = f_decrypt(var_encrypt_var_app_client_id, load_key_external()).decode("utf-8")
var_app_secret = f_decrypt(var_encrypt_var_app_secret, load_key_external()).decode("utf-8")
var_secret_key = f_decrypt(var_encrypt_var_secret_key, load_key_external()).decode("utf-8")

url_sbis = f_decrypt(var_encrypt_url_sbis, load_key_external()).decode("utf-8")
url_sbis_unloading = f_decrypt(var_encrypt_url_sbis_unloading, load_key_external()).decode("utf-8")

var_db_user_name = f_decrypt(var_encrypt_var_db_user_name, load_key_external()).decode("utf-8")
var_db_user_pass = f_decrypt(var_encrypt_var_db_user_pass, load_key_external()).decode("utf-8")

var_db_host = f_decrypt(var_encrypt_var_db_host, load_key_external()).decode("utf-8")
var_db_port = f_decrypt(var_encrypt_var_db_port, load_key_external()).decode("utf-8")

var_db_name = f_decrypt(var_encrypt_var_db_name, load_key_external()).decode("utf-8")

var_db_name_for_upl = f_decrypt(var_encrypt_var_db_name_for_upl, load_key_external()).decode("utf-8")


var_db_schema = f_decrypt(var_encrypt_var_db_schema, load_key_external()).decode("utf-8")

API_sbis = f_decrypt(var_encryptvar_API_sbis, load_key_external()).decode("utf-8")
API_sbis_pass = f_decrypt(var_encrypt_API_sbis_pass, load_key_external()).decode("utf-8")



from modules.api_info import var_encrypt_TOKEN_yandex_users, f_decrypt, load_key_external
from modules.api_info import var_encrypt_var_login_da, var_encrypt_var_pass_da
# ____________________________________________________________________________________________

var_TOKEN = f_decrypt(var_encrypt_TOKEN_yandex_users, load_key_external()).decode("utf-8")
login_da = f_decrypt(var_encrypt_var_login_da, load_key_external()).decode("utf-8")
pass_da = f_decrypt(var_encrypt_var_pass_da, load_key_external()).decode("utf-8")



local_tz = pendulum.timezone("Europe/Moscow")


default_arguments = {
    'owner': 'evgenijgrinev',
}


with DAG(
    'Test_generic_transfer_and_merge_sbis_upl_2000_2025',
    schedule_interval='0 11 * * *',
    # schedule_interval='@once',
    catchup=False,
    default_args=default_arguments,
    start_date=pendulum.datetime(2024,7,1, tz=local_tz),
) as dag:


    def start_employees_upl():
        
        # _____________________________________________________
        # _____________________________________________________
        # _____________________________________________________
        
        def emp_append():
            inside_id.append(var_inside_id)
            external_id.append(var_external_id)
            last_name.append(var_last_name)
            first_name.append(var_first_name)
            surname.append(var_surname)
            full_name.append(var_full_name)
        
            # phone_number.append(list_phone_number)
            # work_number.append(list_work_number)
            # email.append(list_email)
            
            position_name.append(var_position_name)
            dep_id.append(var_dep_id)
            dep_name.append(var_dep_name)
            
            accepted.append(var_accepted)
            fired.append(var_fired)
            
            tab_number.append(var_tab_number)
            system_access.append(var_system_access)
            last_activity.append(var_last_activity)
            
            pass_quantity.append(var_pass_quantity)
            pass_id.append(var_pass_id)
            pass_subtype.append(var_pass_subtype)
            pass_type.append(var_pass_type)
        
        # _____________________________________________________
        # _____________________________________________________
        # _____________________________________________________
        
        url = "https://online.sbis.ru/auth/service/" 
        
        method = "СБИС.Аутентифицировать"
        params = {
              "Параметр": {
                 "Логин": f"{API_sbis}",
                 "Пароль": f"{API_sbis_pass}"
              }
        
        }
        parameters = {
           "jsonrpc": "2.0",
           "method": method,
           "params":params,
           "id": 0
        }
            
        response = requests.post(url, json=parameters)
        response.encoding = 'utf-8'
        
        
        str_to_dict = json.loads(response.text)
        access_token = str_to_dict["result"]
        # print("access_token:", access_token)
        
        headers = {
        "X-SBISSessionID": access_token,
        "Content-Type": "application/json",
        }  
        
        # _____________________________________________________________
        
        parameters_real = {
           "jsonrpc": "2.0",
           "method": "СБИС.СписокСотрудников",
           "params": {
              "Параметр": {
                 "Фильтр": {
                    # "НашаОрганизация": {
                    #    "СвЮЛ": {
                    #       "ИНН": "9102236925",
                    #       "КПП": "910201001"
                    #    }
                    # }
                 },
                 # "Навигация": {
                 #    "РазмерСтраницы": "2",
                 #    "Страница": "0"
                 # }
              }
           },
           "id": 1
        }
        
        url_real = f"{url_sbis_unloading}"
        
        response_points = requests.post(url_real, json=parameters_real, headers=headers)
        str_to_dict_points = json.loads(response_points.text)
        
        # _____________________________________________________
        # _____________________________________________________
        # _____________________________________________________
        
        inside_id = []
        external_id = []
        last_name = []
        first_name = []
        surname = []
        full_name = []
        
        # phone_number = []
        # work_number = []
        # email = []
        
        position_name = []
        dep_id = []
        dep_name = []
        
        accepted = []
        fired = []
        
        tab_number = []
        system_access = []
        last_activity = []
        
        pass_quantity = []
        pass_id = []
        pass_subtype = []
        pass_type = []
        
        
        var_inside_id = "",
        var_external_id = "",
        var_last_name = "",
        var_first_name = "",
        var_surname = "",
        var_full_name = "",
        
        var_phone_number = []
        var_work_number = []
        
        var_position_name = "",
        var_dep_id = "",
        var_dep_name = "",
        
        var_accepted = "",
        var_fired = "",
        
        var_tab_number = "",
        var_system_access = "",
        var_last_activity = "",
        
        var_pass_quantity = "",
        var_pass_id = "",
        var_pass_subtype = "",
        var_pass_type = "",
        
        
        lst_name = [
            "var_inside_id",
            "var_external_id",
            "var_last_name",
            "var_first_name",
            "var_surname",
            "var_full_name",
            
            # "var_phone_number",
            # "var_work_number",
            # "email",
        
            "var_position_name",
            "var_dep_id",
            "var_dep_name",
        
            "var_accepted",
            "var_fired",
        
            "var_tab_number",
            "var_system_access",
            "var_last_activity",
        
            "var_pass_quantity",
            "var_pass_id",
            "var_pass_subtype",
            "var_pass_type",
        ]
        
        
        for i in range(len(str_to_dict_points["result"]["Сотрудник"])):
        
            try:
                fio = " ".join([str_to_dict_points["result"]["Сотрудник"][i]["Фамилия"], str_to_dict_points["result"]["Сотрудник"][i]["Имя"], str_to_dict_points["result"]["Сотрудник"][i]["Отчество"]])
            except:
                fio = np.nan
        
            var_inside_id = str_to_dict_points["result"]["Сотрудник"][i]["Идентификатор"]
            var_external_id = str_to_dict_points["result"]["Сотрудник"][i]["ИдентификаторИС"]
            
            var_last_name = str_to_dict_points["result"]["Сотрудник"][i]["Фамилия"]
            var_first_name = str_to_dict_points["result"]["Сотрудник"][i]["Имя"]
            var_surname = str_to_dict_points["result"]["Сотрудник"][i]["Отчество"]
            
            fio = " ".join([str_to_dict_points["result"]["Сотрудник"][i]["Фамилия"], str_to_dict_points["result"]["Сотрудник"][i]["Имя"], str_to_dict_points["result"]["Сотрудник"][i]["Отчество"]])   
            var_full_name = fio
            
            if type(str_to_dict_points["result"]["Сотрудник"][i]["Должность"]) == str:
                var_position_name = str_to_dict_points["result"]["Сотрудник"][i]["Должность"]
            elif type(str_to_dict_points["result"]["Сотрудник"][i]["Должность"]) == dict:        
                var_position_name = str_to_dict_points["result"]["Сотрудник"][i]["Должность"]["Название"]
        
            var_dep_id = str_to_dict_points["result"]["Сотрудник"][i]["Подразделение"]["Идентификатор"]
            var_dep_name = str_to_dict_points["result"]["Сотрудник"][i]["Подразделение"]["Название"]
        
            
            var_accepted = str_to_dict_points["result"]["Сотрудник"][i]["Принят"]
            var_fired = str_to_dict_points["result"]["Сотрудник"][i]["Уволен"]
        
            var_tab_number = str_to_dict_points["result"]["Сотрудник"][i]["ТабельныйНомер"]
            
            var_system_access = str_to_dict_points["result"]["Сотрудник"][i]["ДоступВСистему"]
            
            var_last_activity = str_to_dict_points["result"]["Сотрудник"][i]["ПоследняяАктивность"]
            
            
            if type(str_to_dict_points["result"]["Сотрудник"][i]["Пропуск"]) == dict:
                var_pass_quantity = 1
            elif type(str_to_dict_points["result"]["Сотрудник"][i]["Пропуск"]) == list:
                var_pass_quantity = len(str_to_dict_points["result"]["Сотрудник"][i]["Пропуск"])
                
                q_pass = []
                q_pass_subtype = []
                q_pass_type = []
                
                for j in str_to_dict_points["result"]["Сотрудник"][i]["Пропуск"]:
                    q_pass.append(j["Идентификатор"])
                    q_pass_subtype.append(j["ПодТипПропуска"])                 
                    q_pass_type.append(j["ТипПропуска"])
        
                var_pass_id = q_pass
                var_pass_subtype = q_pass_subtype    
                var_pass_type = q_pass_type
                      
            else:
                var_pass_quantity = np.nan                   
                var_pass_id = np.nan
                var_pass_subtype = np.nan
                var_pass_type = np.nan
        
        
            # parameters_real = {
            #    "jsonrpc": "2.0",
            #    "method": "СБИС.ПрочитатьСотрудника",
            #    "params": {
            #       "Параметр": {
            #          "Сотрудник": {
            #             "Идентификатор": var_inside_id,
        
            #             "ДопПоля":[
            #                "Образование"
            #              ]
            #          }
            #       }
            #    },
            #    "id": 1
            # }
        
            # url_real = f"{url_sbis_unloading}"
            
            # response_points = requests.post(url_real, json=parameters_real, headers=headers)
            # str_to_dict_points_inside = json.loads(response_points.text)
        
            # print(i)
            # print('_________________________________________')
            
        
            # if type(str_to_dict_points_inside["result"]["Сотрудник"]["Контакты"]) == dict:
            #     print(str_to_dict_points_inside)
                
            # elif type(str_to_dict_points_inside["result"]["Сотрудник"]["Контакты"]) == list:
            #         # print(2)
            #         list_phone_number = []
            #         list_work_number = []
            #         list_email = []
                    
            #         for j in str_to_dict_points_inside["result"]["Сотрудник"]["Контакты"]:
            #             # print(j)
            #             if j["Тип"] == 'МобильныйТелефон':
            #                 # print(j["Значение"])
            #                 list_phone_number.append(j["Значение"])
            #             elif j["Тип"] == 'РабочийТелефон':
            #                 # print(j["Значение"])
            #                 list_work_number.append(j["Значение"])
            #             elif j["Тип"] == 'ЭлПочта':
            #                 # print(j["Значение"])
            #                 list_email.append(j["Значение"])
            #                 # var_work_number.append(j["Значение"])
        
                            
            
        
            emp_append()
        
        
        # _____________________________________________________
        # _____________________________________________________
        # _____________________________________________________
        
        df_emp = pd.DataFrame(columns=lst_name, data=list(zip(
                inside_id,
                external_id,
                last_name,
                first_name,
                surname,
                full_name,
                
                # phone_number,
                # work_number,
                # email,
                
                position_name,
                dep_id,
                dep_name,
                
                accepted,
                fired,
                
                tab_number,
                system_access,
                last_activity,
                
                pass_quantity,
                pass_id,
                pass_subtype,
                pass_type,
            ))
        )
        
        # _____________________________________________________
        # _____________________________________________________
        # _____________________________________________________
        df_sbis_empl = pd.read_excel("/mnt/da_server/sbis_employees/Сотрудники.xlsx")
        # df_sbis_empl = pd.read_excel("Z:/sbis_employees/Сотрудники.xlsx")
        df_sbis_empl_fil = df_sbis_empl[(~df_sbis_empl["Ссылка на карточку"].isin([' ', ''])) & (~df_sbis_empl["Ссылка на карточку"].isna())]
        df_sbis_empl_fil.columns.to_list()
        
        # _____________________________________________________
        # _____________________________________________________
        # _____________________________________________________
        
        col_realise = [
            'ФИО',
            'Логин',
            'Ссылка на карточку',
         
            'Должность',
            'Роли',
            'Название подразделения',
            'Код подразделения',
            'Полный путь до подразделения',
            'ФИО руководителя',
            'Должность руководителя',
            
            'Рабочий телефон',
            'Мобильный телефон',
            'Email',
        ]
        
        df_sbis_empl_fil_red = df_sbis_empl_fil[col_realise].reset_index(drop=True)
        df_sbis_empl_fil_red["ФИО"] = df_sbis_empl_fil_red["ФИО"].apply(lambda x: x.strip())
        
        # _____________________________________________________
        # _____________________________________________________
        # _____________________________________________________
        
        df_emp_merge = df_emp.drop_duplicates(["var_full_name"], keep='last').merge(df_sbis_empl_fil_red.drop_duplicates(["ФИО"], keep='last'), how='left', left_on=["var_full_name"], right_on=["ФИО"])
        
        # _____________________________________________________
        # _____________________________________________________
        # _____________________________________________________
        
        
        my_conn = create_engine(f"postgresql+psycopg2://{var_db_user_name}:{var_db_user_pass}@{var_db_host}:{var_db_port}/{var_db_name_for_upl}")
        try: 
            my_conn.connect()
            print('connect')
            my_conn = my_conn.connect()
            df_emp_merge.to_sql(name=f'sbis_list_emp', con=my_conn, schema=f'{var_db_schema}', if_exists="replace")
            print('success!')
            my_conn.close()
        except:
            print('failed')
        #  __________________________________________________
    
    op_upl = PythonOperator(
        task_id='launch_start_employees_upl',
        python_callable=start_employees_upl
    )

op_upl

