import os
from pathlib import Path
import pandas as pd
from unidecode import unidecode
import warnings

def transformar_dataframe():
    # Desliga Warling
    # warnings.simplefilter(action='ignore', category=FutureWarning)
    warnings.simplefilter(action='ignore', category=FutureWarning)
    warnings.simplefilter(action='ignore', category=pd.errors.DtypeWarning)
    
    # Caminhos das pastas de origem e destino
    path_download = f"{Path(__file__).resolve().parent}\\Files_Download\\" 
    path_normalized = f"{Path(__file__).resolve().parent}\\Files_Normalized\\" 

    # Dicionário de mapeamento de colunas
    colunas_mapeadas = {
        '_source.paciente_endereco_uf': 'CD_UF',
        '_source.paciente_enumSexoBiologico': 'CD_SEXO_BIOLOGICO',
        '_source.paciente_endereco_nmMunicipio': 'NM_CIDADE',
        '_source.vacina_nome': 'NM_VACINA',
        '_source.vacina_descricao_dose': 'DS_DOSE',
        '_source.vacina_dataAplicacao': 'DT_APLICACAO'
    }

    # Listar todos os arquivos na pasta de origem
    arquivos = [f for f in os.listdir(path_download) if f.endswith('.csv')]

    if len(arquivos) > 0:
        for arquivo in arquivos:
            caminho_arquivo = os.path.join(path_download, arquivo)
            
            # Ler o arquivo CSV
            df = pd.read_csv(caminho_arquivo, sep=";")
            
            # Retira lixo
            df = df[df['_source.dt_deleted'].isna()]
            df = df[df['_source.vacina_dataAplicacao'].notna()]
            
            # Filtrar as colunas que estão no dicionário de mapeamento
            df_filtrado = df[list(colunas_mapeadas.keys())]

            # Renomear as colunas de acordo com o dicionário de mapeamento
            df_renomeado = df_filtrado.rename(columns=colunas_mapeadas)

            # Remover acentos e converter para maiúsculas
            df_transformado = df_renomeado.applymap(lambda x: unidecode(str(x)).upper() if isinstance(x, str) else x)
            
            # Caminho para salvar o novo arquivo
            caminho_novo_arquivo = os.path.join(path_normalized, arquivo)
            
            # Salvar o DataFrame transformado na pasta de destino
            df_transformado.to_csv(caminho_novo_arquivo, sep=";", index=False)
            
            print("-------------------------------------------------------")
            print(f"Arquivo Normalizado:\n{caminho_novo_arquivo}")
            print("-------------------------------------------------------")
            
            # Deletar o arquivo original
            os.remove(caminho_arquivo)
            print("-------------------------------------------------------")
            print(f"Arquivo :{caminho_arquivo} deletado")
            print("-------------------------------------------------------")

        print("Todos os arquivos foram processados e movidos com sucesso.")
    
    else:
        print("Nenhum arquivo para normalizar")
