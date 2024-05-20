import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import json
from pathlib import Path
from Error_Log import write_erro_log
from config import data_sus_url, data_sus_url_scroll, data_sus_public_user, data_sus_public_pw
from config import var_max_return_sus_api
# from config import var_dt_inicio, var_dt_fim

def download_covid_data(dt_inicio, dt_fim):
    try:
        url = data_sus_url
        url_scroll = data_sus_url_scroll
        user = data_sus_public_user
        password = data_sus_public_pw
        head = {'content-type' : 'application/json'}
        # dt_inicio = var_dt_inicio
        # dt_fim = var_dt_fim
        file_name = "VacinaCovid_" + dt_inicio.replace("-", "_") + ".csv"
        file_address = f"{Path(__file__).resolve().parent}\\Files_Download\\" + file_name

        # Seta requisição || gte = maior igual ">=" || lt = menor "<"
        payload = {
            "size":var_max_return_sus_api,
                "query": {
                    "range": {
                        "vacina_dataAplicacao": {
                            "gte": dt_inicio,
                            "lt": dt_fim
                        }
                    }
                }
            }
        
        # Requisita para a API os dados
        resposta = requests.post(url, 
                            auth=HTTPBasicAuth(user, password),
                            headers=head,
                            data=json.dumps(payload)
                        )
        # Transforma dados retornados
        var_json = resposta.json()
        
        # Colhe dados, metadados e url de possivel paginação de dados (scroll)
        scroll = var_json['_scroll_id']
        dados_vacina = var_json['hits']['hits']
        total_registros_periodo = var_json['hits']['total']['value']
        
        # Transforma dados para DataFrame
        df_dados_vacina = pd.json_normalize(dados_vacina)

        df_len = len(df_dados_vacina)
        
        # Se houver dados no periodo
        if df_len > 0:
            # Escrever o retorno em um arquivo CSV
            print("----------------------------------------------------")
            print("Escreve retorno")
            print("----------------------------------------------------")
            df_dados_vacina.to_csv(file_address, sep=";", index=False)

            # Se houver mais registros no período que o retornado (paginação)
            if(total_registros_periodo > df_len):
                # Enquanto houver regostros
                while (df_len > 0):
                    payload_scroll = {
                        "scroll_id":scroll,
                        "scroll":"1m"
                        }
                    print("------------------------------------------------------")
                    print(f"Executando paginação: {file_address}")
                    print("------------------------------------------------------")
                    resposta_scroll = requests.post(url_scroll, 
                                        auth=HTTPBasicAuth(user, password),
                                        headers=head,
                                        data=json.dumps(payload_scroll)
                                    )

                    var_json_scroll = resposta_scroll.json()
                    scroll = var_json_scroll['_scroll_id']
                    hits = var_json_scroll['hits']['hits']
                    df = pd.json_normalize(hits)
                    df_len = len(df)
                    df.to_csv(file_address, mode='a', header=False, sep=";", index=False)
        
    except Exception as e:
        write_erro_log(str(e))
