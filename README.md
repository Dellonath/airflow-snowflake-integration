# airflow-snowflake-integration

# Set up
## Airflow Variables need be added

```
dbcreds=dags/.creds/dbcreds.toml
```

# Important Docker commands

```
docker compose up -d
docker compose down -v --rmi all
```

# Parametrization

You can define the dag arguments in the dag configuration file, specifically in `default_args` field. 

The script `dag_facory.py` will then push the `default_args` parameters as parameters (or in `kwargs`) of each task defined in the configuration file. Be aware to make sure you will not replace any default Airflow parameter. 

As example, consider that the follow default_args was defined in dag config file:

```json
"default_args": {
   "owner": "douglas-oliveira",
   "bronze_path": "dags/data/bronze",
   "file_format": "parquet"
}
```

Use them to parametrize the task definition in dag configuration file using double curly braces `{{arg}}`:
```json
{
   "name": "database-extraction-mysql",
   "script": "src.models.database.handler",
   "params": {
      "sql": "SELECT * FROM dellocred_crm.clients",
      "output": "{{bronze_path}}/crm_clients", 
      "file_format": "csv"
   }
}
```

Suppose now that the above task definition creates a task using `database.py`. You can get the parameters in the task as follow:
```python
# database.py
def handler(sql: str, output: str, file_format: str, **kwargs):
   print(sql) # "SELECT * FROM dellocred_crm.clients"
   print(output) # "dags/data/bronze/crm_clients"
   print(file_format) # "csv", replacing the default_args parameter
   print(kwargs.get('owner')) # "douglas-oliveira"
```

Be aware that arguments defined in `default_args` will be reflected to downstream tasks as task parameters. But parameters defined in the task definition in `params` replaces the dags parameters in `default_args`. So you don't need to define same parameter in both dag and task, you can reuse the same parameter across all tasks.

# Code Quality

```
ruff check dags/ --select AIR3
ruff check dags/src/models
```

ruff assists in detecting deprecated features and patterns that may affect your migration to Airflow 3.0. For instance, it includes rules prefixed with AIR to flag potential issues. The full list is detailed in [Airflow (AIR)](https://docs.astral.sh/ruff/rules/#airflow-air).

That's an example of ruff usage, showing what need be changed to achieve an optimal quality code:

```powershell
AIR311 `airflow.DAG` is removed in Airflow 3.0; It still works in Airflow 3.0 but is expected to be removed in a future version.
  --> dags/dag_factory.py:12:10
   |
10 |     with open(f'{YAML_CONFIGS_PATH}/{file}', 'r') as f:
11 |         dag_cfg = json.load(f)
12 |     with DAG(
   |          ^^^
13 |         dag_id=file.split('.')[0],
14 |         catchup=False,
   |
help: Use `DAG` from `airflow.sdk` instead.
```

Ref: [Airflow/Ruffus Code Quality and Linting](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#code-quality-and-linting)
