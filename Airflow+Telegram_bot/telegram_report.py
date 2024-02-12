import pandas as pd
from datetime import datetime, timedelta
import pandahouse as ph
import io
from io import StringIO
import requests
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import seaborn as sns
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import get_current_context
import telegram
import numpy as np

my_token = 'REPORT_BOT_TOKEN' 
bot = telegram.Bot(token=my_token) 

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
                'start_date': datetime(2024, 2, 11),}

chat_id = -123456789

# функция для чтения таблиц из Сlickhouse
def ch_get_df(query):
    result = ph.read_clickhouse(query = query, connection = connection)
    return result

# запускаем каждый день в 11 утра
schedule_interval = '0 11 * * *'

# даг
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_report_kirkir():
    @task()
    # метрики за неделю
    # будем считать наиболее показательные метрики, позволяющие оперативно среагировать в случае, если что-то сломается
    def extract_week():
        query = """SELECT day, DAU, views, likes, CTR, count_messages, count_users_organic, count_users_ads FROM
                   (SELECT count (distinct user_id) as DAU,
                   countIf(action='view') as views,
                   countIf(action='like') as likes, 
                   round(countIf(action='like') / countIf(action='view'), 2) as CTR,
                   countIf(distinct user_id, source='organic') as count_users_organic,
                   countIf(distinct user_id, source='ads') as count_users_ads,
                   toDate(time) as day
                   FROM simulator_20240120.feed_actions
                   WHERE toDate(time) between today() - 8 and today() - 1
                   group by day) as t1
                   LEFT JOIN 
                   (SELECT count(user_id) as count_messages, toDate(time) as day
                   FROM simulator_20240120.message_actions
                   WHERE toDate(time) between today() - 8 and today() - 1
                   group by day) as t2
                   ON t1.day = t2.day
                   """

        df_week = ch_get_df(query)
        return df_week
    
    @task()
    # total, запрос, отвечающий на вопросы сколько пользователей ушло, пришло и осталось по неделям
    def extract_total():
        query = """SELECT this_week, previous_week, -uniq(user_id) as num_users, status FROM
                    (SELECT user_id, 
                    groupUniqArray(toMonday(toDate(time))) as weeks_visited, 
                    addWeeks(arrayJoin(weeks_visited), +1) this_week, 
                    if(has(weeks_visited, this_week) = 1, 'retained', 'gone') as status, 
                    addWeeks(this_week, -1) as previous_week
                    FROM simulator_20240120.feed_actions
                    group by user_id)
                    where status = 'gone'
                    group by this_week, previous_week, status
                    HAVING this_week != addWeeks(toMonday(today()), +1)
                    union all
                    SELECT this_week, previous_week, toInt64(uniq(user_id)) as num_users, status FROM
                    (SELECT user_id, 
                    groupUniqArray(toMonday(toDate(time))) as weeks_visited, 
                    arrayJoin(weeks_visited) this_week, 
                    if(has(weeks_visited, addWeeks(this_week, -1)) = 1, 'retained', 'new') as status, 
                    addWeeks(this_week, -1) as previous_week
                    FROM simulator_20240120.feed_actions
                    group by user_id)
                    group by this_week, previous_week, status"""

        df = ch_get_df(query)
        return df
    
    @task()
    # отправка сообщения
    def send_mess(df_week, df):
        yesterday = (pd.Timestamp.now() - pd.Timedelta(days=1)).normalize()
        df_one_day = df_week[df_week['day'] == yesterday]
        msg = f'Key metrics for {yesterday.date()}:\nDAU: {df_one_day.iloc[0, 1]} (organic: {df_one_day.iloc[0, 6]}, ads: {df_one_day.iloc[0, 7]})\nviews: {df_one_day.iloc[0, 2]}\nlikes: {df_one_day.iloc[0, 3]}\nCTR: {df_one_day.iloc[0, 4]}\ncount messages: {df_one_day.iloc[0, 5]}'
        bot.sendMessage(chat_id=chat_id, text=msg)
        
        # Построение и отправка графиков
        # линейный subplots
        df_week.set_index('day', inplace=True)
        plt.figure(figsize=(6, 3))

        fig, axes = plt.subplots(7, 1, figsize=(10, 12), sharex=True)

        for i, column in enumerate(df_week.columns):
            sns.lineplot(ax=axes[i], x=df_week.index, y=df_week[column], marker='o', legend=False)
            axes[i].set_title(column)
            axes[i].set_xlabel('')
            axes[i].set_ylabel('')
            axes[i].tick_params(axis='x')

        plt.tight_layout()
        plt.suptitle('Metrics Dynamics for the Previous Week', fontsize=16)
        plt.subplots_adjust(top=0.93, hspace=0.3)
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'metrics_week.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        # и второй график, отвечающий на вопросы сколько пользователей ушло, пришло и осталось по неделям
        df['this_week'] = pd.to_datetime(df['this_week']).dt.to_period('D')
        df['previous_week'] = pd.to_datetime(df['previous_week']).dt.to_period('D')
        df['diff_users'] = df['num_users']
        df = df.sort_values(by='status')
        column_order = ['retained', 'new', 'gone']

        # группировка данных по текущей неделе и статусу и подсчет разницы в количестве пользователей
        grouped = df.groupby(['this_week', 'status']).agg({'diff_users': 'sum'}).unstack(fill_value=0)
        grouped.columns = grouped.columns.droplevel()
        grouped = grouped[column_order]

        ax = grouped.plot(kind='bar', stacked=True)
        plt.xlabel('')
        plt.title('Number of New, Gone, and Retained Users by Date')
        handles, labels = ax.get_legend_handles_labels()
        new_order = [1, 0, 2]
        plt.legend([handles[idx] for idx in new_order], [labels[idx] for idx in new_order], title='Status', bbox_to_anchor=(1.05, 1), loc='upper left')

        plt.xticks(rotation=45)
        plt.tight_layout()
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'total.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
    df_week = extract_week()
    df = extract_total()
    send_mess(df_week, df)
    
# и даг готов    
dag_report_kirkir = dag_report_kirkir()

        




