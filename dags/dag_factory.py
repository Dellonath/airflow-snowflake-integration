import os
import json
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from src.models.database import DatabaseConnector
from src.models.snowflake import SnowflakeConnector

YAML_CONFIGS_PATH: str = 'dags/configs'

# add any new script here, poiting the path and the callable function
SCRIPTS: dict[str, callable] = {
    'dags/src/models/database.py': DatabaseConnector.handler,
    'dags/src/models/snowflake.py': SnowflakeConnector.handler
}

for file in os.listdir(YAML_CONFIGS_PATH):
    with open(f'{YAML_CONFIGS_PATH}/{file}', 'r') as f:
        dag_cfg = json.load(f)
    with DAG(
        dag_id=file.split('.')[0],
        catchup=False,
        schedule=dag_cfg.get('schedule'),
        tags=dag_cfg.get('tags'),
        default_args=dag_cfg.get('default_args'),
    ) as dag:
        tasks: dict = {}
        tasks_definition: dict = {task['name']: task for task in dag_cfg.get('tasks', [])}
        for dag_task_name, dag_task_cfg in tasks_definition.items():
            tasks[dag_task_name] = PythonOperator(
                task_id=dag_task_cfg.get('name'),
                python_callable=SCRIPTS.get(dag_task_cfg.get('script')),
                op_kwargs=dag_task_cfg.get('params')
            )
        # setting dependencies between tasks
        for dag_task_name in tasks_definition.keys():
            if 'depends_on' in tasks_definition.get(dag_task_name):
                tasks[dag_task_name] << [tasks.get(id_) for id_ in tasks_definition.get(dag_task_name).get('depends_on')]
