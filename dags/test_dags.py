"""
Copy Mysql Data
"""


from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.mysql_operator import MySqlOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['183@qq.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

delete_all_users_sql = "TRUNCATE TABLE users"
move_tmp_users_sql = "insert into users SELECT * FROM tmp_users"

with DAG(
    dag_id='CopyMysqlData',
    default_args=default_args,
    description='Copy Mysql Data',
    schedule_interval="* * * * 5"
) as dag:
    delete_all_users_task = MySqlOperator(
        task_id='delete_all_users',
        sql=delete_all_users_sql,
        mysql_conn_id='mysql_test_db',
        autocommit=True,
        dag=dag)
    
    move_tmp_users_task = MySqlOperator(
        task_id='move_tmp_users',
        sql=move_tmp_users_sql,
        mysql_conn_id='mysql_test_db',
        autocommit=True,
        dag=dag)
    delete_all_users_task >> move_tmp_users_task




