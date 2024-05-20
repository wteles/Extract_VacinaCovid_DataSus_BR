import os
from pathlib import Path
from crud_OCI import upload_file, oci_delete_file
from Error_Log import write_erro_log
import time
from datetime import datetime
from config import var_oci_bucket_prefix, var_oci_job_semaphore

def send_files_to_oci():
    file_path = f"{Path(__file__).resolve().parent}\\Files_Normalized\\"
    list_files = os.listdir(file_path)

    if len(list_files) > 0:
        oci_delete_file(var_oci_job_semaphore)
        try:
            for file in list_files:
                print("-------------------------------------------------------")
                print("Upload File:")
                print(file)
                file_address = f"{file_path}{file}"
                print(file_address)
                print("-------------------------------------------------------")
                file_bucket_name = f"{var_oci_bucket_prefix}{file}"

                r = upload_file(local_file_path=file_address,object_name=file_bucket_name)
                time.sleep(0.2)
                
                # Se o upload com sucesso, deleta o arquivo local
                if r == 0:
                    print("-------------------------------------------------------------")
                    print(f"Delete: {file_address}")
                    print("-------------------------------------------------------------")
                    os.remove(file_address)
            
            semaphore_address = "semaphre_job.txt"
            with open(semaphore_address, 'w') as arquivo:
                arquivo.write(f"-----------------------------------\n")
                arquivo.write(f"{datetime.now()}\n")
                arquivo.write(f"-----------------------------------")
            
            upload_file(local_file_path=semaphore_address,object_name=var_oci_job_semaphore)
            os.remove(semaphore_address)
        except Exception as e:
            write_erro_log(str(e))

    else:
        print("-------------------------------------------------------")
        print("Não há arquivos para enviar")
        print("-------------------------------------------------------")
