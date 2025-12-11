import tomllib
from airflow.sdk import Variable

def load_toml_creds():
    with open(file=Variable.get('dbcreds'), mode='rb') as f: 
        return tomllib.load(f)