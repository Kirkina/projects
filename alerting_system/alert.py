import pandas as pd
from datetime import datetime, timedelta
import pandahouse as ph
import requests
import numpy as np
import telegram
import io
from io import StringIO
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import get_current_context


my_token = 'REPORT_BOT_TOKEN'
bot = telegram.Bot(token=my_token) 
chat_id = -123456789

# задаём параметры подключения к БД Сlickhouse
connection = {'host': 'https://clickhouse.lab.karpov.courses',
              'database':'simulator_20240120',
              'user':'student',
              'password':'***'}

# дефолтные параметры, которые прокидываются в таски
default_args = {'owner': 't-kirkina',
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=3),
                'start_date': datetime(2024, 2, 16),}

# функция для чтения таблиц из Сlickhouse
def ch_get_df(query):
    result = ph.read_clickhouse(query = query, connection = connection)
    return result

# запускаем DAG каждые 15 минут
schedule_interval = '*/15 * * * *'

# Рассчитаем метрики в разрезе 15-минутных интервалов за последние 2 недели. 
# Затем будем оценивать, является ли отклонение любой из метрик критическим 
# относительно значений в тех же временных интервалах за последние 2 недели

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_kirkir_alerts():
    @task() # Метрики df_actions
    def extract_df_actions():
        query = """SELECT 
            toStartOfInterval(time, INTERVAL 15 MINUTE) AS interval_start,
            count(DISTINCT user_id) AS count_users,
            countIf(action = 'like') AS likes_cnt,
            countIf(action = 'view') AS views_cnt,
            round(countIf(action = 'like') / nullif(countIf(action = 'view'), 0), 2) AS CTR,
            toDate(toStartOfInterval(time, INTERVAL 15 MINUTE)) AS day
        FROM 
            simulator_20240120.feed_actions
        WHERE
            toStartOfInterval(time, INTERVAL 15 MINUTE) < NOW() - INTERVAL 15 MINUTE AND
            time >= NOW() - INTERVAL 2 WEEK
        GROUP BY 
            interval_start
        ORDER BY 
            interval_start;
        """

        df_actions = ch_get_df(query)
        return df_actions
    
    @task() # Метрики message_actions
    def extract_message_actions():
        query = """SELECT 
            toStartOfInterval(time, INTERVAL 15 MINUTE) AS interval_start,
            count(user_id) AS count_messages,
            toDate(toStartOfInterval(time, INTERVAL 15 MINUTE)) AS day
        FROM 
            simulator_20240120.message_actions
        WHERE 
            toStartOfInterval(time, INTERVAL 15 MINUTE) < NOW() - INTERVAL 15 MINUTE AND
            time >= NOW() - INTERVAL 2 WEEK
        GROUP BY 
            interval_start
        ORDER BY 
            interval_start;"""

        df_mess = ch_get_df(query)
        return df_mess
    
    @task() # Объединяем таблицы и отправляем сообщение, если хотя бы 1 метрика упала
    def send_mess(df_actions, df_mess):
        df_actions['time_column'] = df_actions['interval_start'].dt.time.astype('str')
        del df_actions['interval_start']
        df_mess['time_column'] = df_mess['interval_start'].dt.time.astype('str')
        del df_mess['interval_start']
        
        df = pd.merge(df_actions, df_mess, left_on = ['day', 'time_column'], right_on = ['day', 'time_column'], how = 'left')
        df = df.sort_values(by=['day', 'time_column'])
        
        # Выделяем только метрики из последней строки (т.е. за предыдущую 15-минутку)
        last_time_value = df['time_column'].iloc[-1]
        filtered_df = df[df['time_column'] == last_time_value]
        last_metrics = filtered_df.iloc[-1][['count_users', 'likes_cnt', 'views_cnt', 'CTR', 'count_messages']]
        
        # Отбрасываем последнюю строку для вычисления средних значений и стандартных отклонений
        previous_data = filtered_df.iloc[:-1][['count_users', 'likes_cnt', 'views_cnt', 'CTR', 'count_messages']]
        
        # Вычисляем средние значения и стандартные отклонения для каждой метрики
        means = previous_data.mean()
        std_devs = previous_data.std()
        
        # Будем проверять, упала ли метрика ниже lower_bounds
        lower_bounds = means - 3 * std_devs
        
        # Отправляем сообщение, если хотя бы 1 метрика упала
        printed_dashboards = set()
        messages = [] 

        # Печатаем сообщение
        for metric, value in last_metrics.items():
            if value < lower_bounds[metric]:
                percentage = ((value - means[metric]) / means[metric]) * 100
                msg = f"Текущее значение {metric}: {value}. Отклонение более {percentage:.2f}%."
                messages.append(msg) 
                if 'dashboard_link' not in printed_dashboards:
                    printed_dashboards.add('dashboard_link')

        # Печатаем ссылку на дашборд
        if messages: 
            if 'dashboard_link' in printed_dashboards:
                dashboard_link = 'https://superset.lab.karpov.courses/superset/dashboard/4881/'
                link = f"Ссылка на дашборд в BI для исследования ситуации: {dashboard_link}"
                messages.append(link) 
            # Отправляем все сообщения одним запросом
            bot.sendMessage(chat_id=chat_id, text='\n'.join(messages))

    df_actions = extract_df_actions()
    df_mess = extract_message_actions()
    send_mess(df_actions, df_mess)
    
# и даг готов    
dag_kirkir_alerts = dag_kirkir_alerts()
        
    
    