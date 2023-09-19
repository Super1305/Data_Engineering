"""
The DAG uses a plugin made by me to parse TOP 3 locations with highest number of creatures from
some API, creates a table in Greenplum DB as per a connection and writes the results to the table.
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from i_grebenjuk_23_plugins.IG_highestnumber import HighestNumbersOperator

DEFAULT_ARGS = {
    'owner': 'i-grebenjuk-23',
    'start_date': days_ago(1),
    'poke_interval': 600,
    'retries': 1,
    'depends_on_past': True
}

table = 'i_grebenjuk_23_ram_location'
path = '/tmp/top.csv'
sql = f"""
    DROP TABLE IF EXISTS {table};

    CREATE TABLE IF NOT EXISTS {table} (
        id            Int4 PRIMARY KEY,
        name          VARCHAR,
        type          VARCHAR,
        dimension     VARCHAR,
        resident_cnt  Int4                   
    )
    DISTRIBUTED BY (id);
    """

with DAG(
        dag_id='IG_04',
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['IG','04_dag']
) as dag:

    top_locations = HighestNumbersOperator(
        task_id='top_locations',
        path=path
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        sql=sql
    )

    def load_data_to_gp_func():
        pg_hook = PostgresHook('conn_greenplum_write')
        pg_hook.copy_expert(f"COPY {table} FROM STDIN DELIMITER ','", path)

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data_to_gp_func
    )

top_locations >> create_table >> load_data