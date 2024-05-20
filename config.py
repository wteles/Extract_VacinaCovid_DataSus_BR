from dotenv import dotenv_values
from pathlib import Path
import json

# Carregar as variáveis do arquivo .env
env_address = f"{Path(__file__).resolve().parent}\\.env"
env_vars = dotenv_values(env_address)

data_sus_url = env_vars.get("DATA_SUS_URL")
data_sus_url_scroll = env_vars.get("DATA_SUS_URL_SCROLL")
data_sus_public_user = env_vars.get("DATA_SUS_PUBLIC_USER")
data_sus_public_pw = env_vars.get("DATA_SUS_PUBLIC_PASSWORD")

oci_user = env_vars.get("OCI_USER")
oci_fingerprint = env_vars.get("OCI_FINGERPRINT")
oci_tenancy = env_vars.get("OCI_TENANCY")
oci_region = env_vars.get("OCI_REGION")
oci_bucketname = env_vars.get("OCI_BUCKET_NAME")

# Carregar variáveis
variables_address = f"{Path(__file__).resolve().parent}\\variaveis.json"
with open(variables_address, 'r') as arquivo:
    variaveis = json.load(arquivo)

var_dt_inicio = variaveis["Settings"][0]["dt_inicio"]
var_dt_fim = variaveis["Settings"][0]["dt_fim"]
var_oci_bucket_prefix = variaveis["Settings"][0]["oci_bucket_prefix"]
var_oci_job_semaphore= variaveis["Settings"][0]["oci_job_semaphore"]
var_max_return_sus_api = variaveis["Settings"][0]["max_return_sus_api"]
var_max_treading_data_sus = variaveis["Settings"][0]["max_treading_data_sus"]
