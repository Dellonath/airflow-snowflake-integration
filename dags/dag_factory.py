import os
import json
import datetime as datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from scripts.database import Database

YAML_CONFIGS_PATH = 'config'
SCRIPTS = {
    'scripts/database.py': Database().run
}

for file in os.listdir(YAML_CONFIGS_PATH):

    with open(f'{YAML_CONFIGS_PATH}/{file}', 'r') as f:
        cfg = json.load(f)

    with DAG(
        dag_id=file.split('.')[0],
        schedule=cfg.get('schedule'),
        tags=cfg.get('tags'),
        default_args=cfg.get('default_args'),
        catchup=False
    ) as dag:
        tasks = {}
        tasks_lookup = {task['id']: task for task in cfg.get('tasks', [])}
        for task_id, task_cfg in tasks_lookup.items():
            task_name = task_cfg.get('name')
            task_script = task_cfg.get('script')
            task_params = task_cfg.get('params')
            tasks[task_id] = PythonOperator(
                task_id=task_name,
                python_callable=SCRIPTS.get(task_script),
                op_kwargs=task_params
            )

        # setting dependencies between tasks
        for task_id in tasks_lookup.keys():
            if 'depends' in tasks_lookup.get(task_id):
                tasks[task_id] >> [tasks.get(id_) for id_ in tasks_lookup.get(task_id).get('depends')]
        
