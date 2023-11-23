from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta

import json

default_args={
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def transform_quote(**kwargs):
    ti = kwargs['ti']
    return_value = ti.xcom_pull(task_ids='extract_quote')
    response_data = json.loads(return_value)

    if isinstance(response_data, list) and len(response_data) > 0:
        quote = response_data[0]

        transformed_quote = {
            "_id": quote.get("_id", ""),
            "content": quote.get("content", ""),
            "author": quote.get("author", ""),
            "tags": quote.get("tags", []),
            "datetime_extracted": quote.get("dateAdded", "")
        }
    else:
        transformed_quote = {}

    return transformed_quote

def save_to_bigquery(**kwargs):
    ti = kwargs['ti']
    transformed_quote = ti.xcom_pull(task_ids='transform_quote')

    if transformed_quote:
        project_id = 'euphoric-coast-400712'
        dataset_id = 'random_quote'
        table_id = 'quotes'

        tags_str = ','.join(transformed_quote['tags'])

        sql = f"""
            INSERT INTO `{project_id}.{dataset_id}.{table_id}`
            (_id, content, author, tags, datetime_extracted)
            VALUES (
                '{transformed_quote["_id"]}',
                '{transformed_quote["content"]}',
                '{transformed_quote["author"]}',
                SPLIT('{tags_str}', ', '),
                '{transformed_quote["datetime_extracted"]}'
            )
        """

        insert_task = BigQueryInsertJobOperator(
            task_id="insert_to_bigquery",
            configuration={
                "query": {
                    "query": sql,
                    "useLegacySql": False
                }
            },
            dag=dag
        )
        insert_task.execute(context=kwargs)


with DAG(
    'random_quote_dag',
    default_args=default_args,
    description='Random Quote from JSON API to BigQuery',
    schedule_interval='0 */11 * * *',
    start_date=datetime(2023, 11, 7),
    tags=['project'],
) as dag:
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='quote_api',
        method='GET',
        endpoint='quotes/random',
        mode='poke',
        timeout=10,
        poke_interval=60,
        dag=dag,
    )

    extract_quote = SimpleHttpOperator(
        task_id='extract_quote',
        method='GET',
        http_conn_id='quote_api',
        endpoint='quotes/random',
        dag=dag,
    )

    transform_quote = PythonOperator(
        task_id="transform_quote",
        python_callable=transform_quote,
        provide_context=True,
        dag=dag
    )

    save_to_bigquery_task = PythonOperator(
        task_id="save_to_bigquery",
        python_callable=save_to_bigquery,
        provide_context=True,
        dag=dag
    )

is_api_available >> extract_quote >> transform_quote >> save_to_bigquery_task

