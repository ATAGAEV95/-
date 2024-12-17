from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from clickhouse_driver import Client


def clickhouse_conn():
    client = Client(host='192.168.0.147', user='admin', password='admin', database='sistema3')
    query = """
    INSERT INTO sistema3.sales_advertising
    SELECT sa.sale_date, sa.order_id, sa.product_id, c.category_id, c.product_name,
        c.category_name, sa.sale_amount, sa.ad_amount
    FROM (
        SELECT  IF(s.sale_date = toDateTime64('1970-01-01', 9), a.ad_date, s.sale_date) AS sale_date, 
            s.order_id, IF(s.product_id = 0, a.product_id, s.product_id) as product_id,
            s.sale_amount, a.ad_amount
        FROM sistema1.sales s 
        FULL JOIN sistema2.advertising a ON
            s.sale_date = a.ad_date AND s.product_id = a.product_id
        ORDER BY sale_date, s.order_id) AS sa
    JOIN sistema1.category c ON
        sa.product_id = c.product_id
    WHERE c.insert_date = yesterday() AND 
        sa.sale_date != yesterday();
    """
    client.execute(query)
    client.disconnect()


default_args = {
    'owner': 'ClickHouse',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': 60,
    'start_date': datetime(2024, 12, 17)
}

with DAG(
    'category_update',
    default_args=default_args,
    schedule_interval=None,
    # schedule_interval='0 13 * * *',
    catchup=True,
    tags=['sales_advertising'],
) as dag:

    task1 = PythonOperator(
        task_id='clickhouse_conn',
        python_callable=clickhouse_conn,
    )

task1
