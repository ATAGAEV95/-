from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from clickhouse_driver import Client


def clickhouse_conn():
    client = Client(host='192.168.0.147', user='admin', password='admin', database='sistema3')
    query = """
    INSERT INTO sistema3.sales_advertising
    SELECT sa.sale_date, sa.order_id, sa.product_id, sa.category_id, 
        sa.product_name, sa.category_name, sa.sale_amount, sa.ad_amount
    FROM sistema3.sales_advertising sa
    JOIN sistema1.category c ON
        sa.product_id = c.product_id
    WHERE sa.category_id != c.category_id AND 
        c.insert_date = yesterday()
    """
    client.execute(query)
    client.disconnect()


default_args = {
    'owner': 'ClickHouse',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': 60,
    'start_date': datetime(2024, 12, 18)
}

with DAG(
    'category_update',
    default_args=default_args,
    schedule_interval='0 13 * * *',
    catchup=True,
    tags=['sales_advertising'],
) as dag:

    task1 = PythonOperator(
        task_id='clickhouse_conn',
        python_callable=clickhouse_conn,
    )

task1
