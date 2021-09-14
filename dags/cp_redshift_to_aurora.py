"""
Copy Redshift data to Aurora_db
1.reset Aurora_db.redshift_users table
2.copy data from redshift to aurora
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.mysql_operator import MySqlOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['dingwen@nd.com.cn'],
    'email_on_failure': True,
    'email_on_retry': False,
}

delete_aurora_all_users_sql = "TRUNCATE TABLE users"

def copy_data():
    pghook = PostgresHook(postgres_conn_id="redshift")
    users = pghook.get_records("SELECT * from dingwen.users")

    mysql = MySqlHook(mysql_conn_id="mysql_test_db")
    mysql.insert_rows(table="redshift_users", rows=users)

with DAG(
    dag_id='Copy Redshift to Aurora_db',
    default_args=default_args,
    description='Copy Mysql Data',
    schedule_interval="* * * * 5"
) as dag:
    delete_aurora_all_users = MySqlOperator(
        task_id='delete_all_users',
        sql=delete_aurora_all_users_sql,
        mysql_conn_id='mysql_test_db',
        autocommit=True,
        dag=dag)
    
    copy_redshift_to_aurora = PythonOperator(
        task_id='copy_redshift_to_aurora',
        python_callable=copy_data,
    )

    delete_aurora_all_users >> copy_redshift_to_aurora