import os
import json
import importlib
from pathlib import Path
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

DAGS_DEFINITION_JSON_PATH: Path = Path('dags/configs')

def replace_default_args_with_task_args(task_args: dict, dag_args: dict):
    # update dag args with task args (priority for task args) dinamically
    dag_args_cp = dag_args.copy()
    for key, value in task_args.items():
        dag_args_cp[key] = value
    return dag_args_cp

for file in os.listdir(DAGS_DEFINITION_JSON_PATH):
    with open(DAGS_DEFINITION_JSON_PATH / file, 'r') as f:
        dag_cfg = json.load(f)
    with DAG(
        dag_id=file.split('.')[0],
        catchup=False,
        schedule=dag_cfg.get('schedule'),
        tags=dag_cfg.get('tags'),
        default_args=dag_cfg.get('default_args'),
        user_defined_macros=dag_cfg.get('default_args')
    ) as dag:
        tasks: dict = {}
        tasks_definition: dict = {task['name']: task for task in dag_cfg.get('tasks', [])}
        for dag_task_name, dag_task_cfg in tasks_definition.items():
            module, func = dag_task_cfg.get('script').rsplit('.', 1)
            tasks[dag_task_name] = PythonOperator(
                task_id=dag_task_cfg.get('name'),
                python_callable=getattr(importlib.import_module(module), func),
                op_kwargs=replace_default_args_with_task_args(
                    task_args=dag_task_cfg.get('params'), 
                    dag_args=dag_cfg.get('default_args')
                )
            )
        # setting dependencies between tasks
        for dag_task_name in tasks_definition.keys():
            if 'depends_on' in tasks_definition.get(dag_task_name):
                tasks[dag_task_name] << [tasks.get(id_) for id_ in tasks_definition.get(dag_task_name).get('depends_on')]
