from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import json
import pandas as pd
import psycopg2
from email import message
import smtplib

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date':  datetime(2023, 9, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'airbox_data_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

def enviar():
    try:
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login('sabrinarodriguez.s@gmail.com','ikyi aljt sxso bmyt')
        subject='La tarea de carga completo'
        body_text='Se ha cargado la data en una base de datos Postgres'
        message='Subject: {}\n\n{}'.format(subject,body_text)
        x.sendmail('sabrinarodriguez.s@gmail.com','sabrina.rodriguez@yapo.cl',message)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')


send_email_task=PythonOperator(
    task_id='dag_envio',
    python_callable=enviar,
    dag=dag,
)


def my_python_function():
    url = "https://pm25.lass-net.org/API-1.0.0/project/airbox/latest/"
    response = requests.get(url)
    data = json.loads(response.text)

    feeds = data["feeds"]
    df = pd.DataFrame(feeds)
    df = df.head(300)

    df['timestamp'] = df['timestamp'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ"))
    df['date'] = pd.to_datetime(df['date'])
    df['time'] = pd.to_datetime(df['time'])

    # List of numeric columns to replace N/A (NaN or None) values with 0
    numeric_columns = ['gps_alt', 'gps_fix', 'gps_lat', 'gps_lon', 'gps_num', 's_d0', 's_d1', 's_d2', 's_h0', 's_t0', 'hcho']
    df[numeric_columns] = df[numeric_columns].fillna(0)

    # List of object-type columns to replace "N/A" values with "null"
    object_columns = ['model', 'device_id', 'c_d0', 'c_d0_method', 'SiteAddr', 'fw_ver', 'name', 'SiteName', 'app', 'area']
    df[object_columns] = df[object_columns].replace('N/A', 'null')
    df[object_columns] = df[object_columns].fillna("null")

    # Establish a connection to the PostgreSQL database
    dbname = 'data-engineer-database'
    host = 'localhost'
    port = '5432'
    user = 'postgres'
    password = '1234'

    conn = psycopg2.connect(
        dbname=dbname,
        host=host,
        port=port,
        user=user,
        password=password
    )

    cursor = conn.cursor()

    # Create a table if it doesn't exist
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS sabrinarodriguez_s_coderhouse.airbox_data (
            time TIMESTAMP,  -- Adjust the data type if 'time' column is present
            timestamp TIMESTAMP,
            SiteName VARCHAR(100),
            app VARCHAR(100),
            area VARCHAR(100),
            date DATE,
            gps_alt NUMERIC,
            gps_fix NUMERIC,
            gps_lat NUMERIC,
            gps_lon NUMERIC,
            gps_num NUMERIC,
            name VARCHAR(100),
            s_d0 NUMERIC,
            s_d1 NUMERIC,
            s_d2 NUMERIC,
            s_h0 NUMERIC,
            s_t0 NUMERIC,
            device_id VARCHAR(100),
            c_d0 NUMERIC,
            c_d0_method VARCHAR(100),
            SiteAddr VARCHAR(100),  -- Add the new columns to the table definition
            fw_ver VARCHAR(100),
            hcho NUMERIC,
            model VARCHAR(100)
        );
    '''
    cursor.execute(create_table_query)
    conn.commit()

    # Insert data into the table
    table_name = 'sabrinarodriguez_s_coderhouse.airbox_data'
    for _, row in df.iterrows():
        insert_query = f"""
            INSERT INTO {table_name} (time, timestamp, SiteName, app, area, date, gps_alt, gps_fix, gps_lat, gps_lon, gps_num, name, s_d0, s_d1, s_d2, s_h0, s_t0, device_id, c_d0, c_d0_method, SiteAddr, fw_ver, hcho, model)
            VALUES (
                '{row['time']}', '{row['timestamp']}', '{row['SiteName']}', '{row['app']}', '{row['area']}',
                '{row['date']}', {row['gps_alt']}, {row['gps_fix']}, {row['gps_lat']}, {row['gps_lon']},
                {row['gps_num']}, '{row['name']}', {row['s_d0']}, {row['s_d1']}, {row['s_d2']},
                {row['s_h0']}, {row['s_t0']}, '{row['device_id']}', {row['c_d0']}, '{row['c_d0_method']}',
                '{row['SiteAddr']}', '{row['fw_ver']}', {row['hcho']}, '{row['model']}'
            );
        """
        cursor.execute(insert_query)
    conn.commit()

run_this = PythonOperator(
    task_id='my_task',
    python_callable=my_python_function,
    dag=dag,
)

run_this >> send_email_task