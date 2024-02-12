import pandas as pd
from datetime import datetime, timedelta
import pandahouse as ph
from io import StringIO
import requests
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import get_current_context

# задаём параметры подключения к БД Сlickhouse
connection = {'host': 'https://clickhouse.lab.karpov.courses',
              'database':'simulator_20240120',
              'user':'student',
              'password':'***'}

# параметры для БД, в которую будем заливать обработанную таблицу
connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                  'database':'test',
                  'user':'student-rw', 
                  'password':'***'}

# дефолтные параметры, которые прокидываются в таски
default_args = {'owner': 't-kirkina',
                'depends_on_past': False,
                'retries': 3,
                'retry_delay': timedelta(minutes=3),
                'start_date': datetime(2024, 2, 8),}

# функция для чтения таблиц из Сlickhouse
def ch_get_df(query):
    result = ph.read_clickhouse(query = query, connection = connection)
    return result

# интервал запуска DAG
schedule_interval = '0 13 * * *'

# даг
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_kirr_t_al():
    
    @task()  # в feed_actions для каждого юзера посчитаем число просмотров и лайков контента за вчера
    def extract_actions():
        query = """SELECT user_id,
                   countIf(action='like') as likes,
                   countIf(action='view') as views
                   FROM simulator_20240120.feed_actions
                   WHERE toDate(time) = toDate(yesterday())
                   GROUP BY user_id"""

        df_feed = ch_get_df(query)
        return df_feed
   
    @task() # В message_actions считаем, сколько он получает и отсылает сообщений + скольким людям он пишет, сколько людей пишут ему
    def extract_messages():
        query =  """SELECT user_id, receiver_id, messages_received, users_received, messages_sent, users_sent
                    FROM 
                    (SELECT receiver_id,
                            COUNT(user_id) AS messages_received,
                            COUNT(distinct user_id) AS users_received
                    FROM simulator_20240120.message_actions
                    WHERE toDate(time) = toDate(yesterday())
                    GROUP BY receiver_id) AS sent

                    FULL OUTER JOIN

                    (SELECT user_id, 
                            COUNT(receiver_id) AS messages_sent,
                            COUNT(distinct receiver_id) AS users_sent
                    FROM simulator_20240120.message_actions
                    WHERE toDate(time) = toDate(yesterday())
                    GROUP BY user_id) AS received

                    ON sent.receiver_id = received.user_id"""
        df_mess = ch_get_df(query)

        # нашлись пользователи, которым что-то писали в сообщения, но при этом они сами сообщений не писали
        # таких кользователей перенесем в столбец user_id
        df_mess.loc[df_mess['user_id'] == 0, 'user_id'] = df_mess['receiver_id']
        del df_mess['receiver_id']
        return df_mess
    
    @task() # объединяем две таблицы в одну, пропуски по юзерам, которые не пользовались сообщениями/лентой, заполняем 0
    def df_merge(df_feed, df_mess):
        df_all = pd.merge(df_feed, df_mess, left_on = ['user_id'], right_on = ['user_id'], how = 'outer').fillna(0)
        return df_all
    
    @task() # выгружаем признаки по пользователям из БД - возьмем неделю, т.к. не все пользователи за вчера пользовались сообщениями
    def extract_feature():
        query =  """SELECT user_id, gender, age, os
                    FROM simulator_20240120.feed_actions
                    WHERE toDate(time) between today() - 8 and today() - 1"""
        df_feature = ch_get_df(query).drop_duplicates()
        return df_feature
    
    @task() # присоединяем их к нашей таблице
    def df_merge_itog(df_all, df_feature):
        df_itog = pd.merge(df_all, df_feature, left_on = ['user_id'], right_on = ['user_id'], how = 'left')
        return df_itog
    
    @task() # срез по полу
    def gender_part(df_itog):
        df_gender = df_itog.groupby('gender').agg({'messages_received':'sum', 'users_received':'sum',
                                    'messages_sent':'sum', 'users_sent':'sum',
                                    'views':'sum', 'likes':'sum'}
                                                 ).reset_index().rename(columns={'gender': 'dimension_value'})
        df_gender['dimension'] = 'gender'
        return df_gender
    
    @task() # срез по OC
    def os_part(df_itog):
        df_os = df_itog.groupby('os').agg({'messages_received':'sum', 'users_received':'sum',
                                           'messages_sent':'sum', 'users_sent':'sum',
                                           'views':'sum', 'likes':'sum'}
                                         ).reset_index().rename(columns={'os': 'dimension_value'})
        df_os['dimension'] = 'os'
        return df_os
    
    @task() # срез по возрасту
    def age_part(df_itog):
        df_age = df_itog.groupby('age').agg({'messages_received':'sum', 'users_received':'sum',
                                             'messages_sent':'sum', 'users_sent':'sum',
                                             'views':'sum', 'likes':'sum'}
                                           ).reset_index().rename(columns={'age': 'dimension_value'})
        df_age['dimension'] = 'age'
        return df_age
    
    @task() # объединяем срезы
    def df_all_part(df_gender, df_os, df_age):
        df = pd.concat([df_os, df_gender, df_age], ignore_index=True)
        df['event_date'] = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        df = df[['event_date', 'dimension', 'dimension_value', 'views', 'likes', 'messages_received', 
                 'messages_sent', 'users_received', 'users_sent']]
        dtype_mapping = {'event_date': 'object',
                         'dimension': 'object',
                         'dimension_value': 'object',
                         'views': 'int32',
                         'likes': 'int32',
                         'messages_received': 'int32',
                         'messages_sent': 'int32',
                         'users_received': 'int32',
                         'users_sent': 'int32'}
        df = df.astype(dtype_mapping)
        return df
    
    @task() # создаем таблицу в БД для последующей заливки обработанных данных  
    def load(df):
        query_test = """CREATE TABLE IF NOT EXISTS test.table_kirkina_tanya_dag(
                        event_date String,
                        dimension String,
                        dimension_value String,
                        views UInt32,
                        likes UInt32,
                        messages_received UInt32,
                        messages_sent UInt32,
                        users_received UInt32,
                        users_sent UInt32
                    ) ENGINE = MergeTree()
                    ORDER BY event_date"""
        ph.execute(query_test, connection=connection_test)
        result = ph.to_clickhouse(df, table = 'table_kirkina_tanya_dag', index = False, connection = connection_test)
        
    df_feed = extract_actions()
    df_mess = extract_messages()
    df_all = df_merge(df_feed, df_mess)
    df_feature = extract_feature()
    df_itog = df_merge_itog(df_all, df_feature)
    df_gender = gender_part(df_itog)
    df_os = os_part(df_itog)
    df_age = age_part(df_itog)
    df = df_all_part(df_gender, df_os, df_age)
    load(df)
# и даг готов    
dag_kirr_t_al = dag_kirr_t_al()
