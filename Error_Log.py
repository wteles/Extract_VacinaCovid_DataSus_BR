from pathlib import Path
import os
from datetime import datetime
import threading

# Lock para garantir o log em caso de multTheading
txt_lock = threading.Lock()

def write_erro_log(erro_log):
    dh_erro = datetime.now()
    str_dh_erro = dh_erro.strftime("%Y-%m-%d %H:%M:%S.%f")
    erro_file_address = f"{Path(__file__).resolve().parent}\\_error_log\\error_log.txt"
    print("--------------------------------------------------------")
    print("Ocorreu um erro:", erro_log)
    print("--------------------------------------------------------")
    
    # Se o arquivo já existe, adiciona erro log
    if os.path.exists(erro_file_address):
        with txt_lock:
            with open(erro_file_address, "a") as file:
                file.write(f"--------------------------------------------------------\n")
                file.write(f"{str_dh_erro}\n")
                file.write(f"--------------------------------------------------------\n")
                file.write(f"{erro_log}\n")
    
    # Se o arquivo não existe, cria arquivo e adiciona erro
    else:
        with txt_lock:
            with open(erro_file_address, "w") as file:
                file.write(f"--------------------------------------------------------\n")
                file.write(f"{str_dh_erro}\n")
                file.write(f"--------------------------------------------------------\n")
                file.write(f"{erro_log}\n")
