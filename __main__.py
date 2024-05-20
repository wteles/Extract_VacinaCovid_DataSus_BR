from datetime import datetime
from dateutil.relativedelta import relativedelta
from threading import Thread, Semaphore
from Extract_Covid_Data import download_covid_data
from Transform_Covid_data import transformar_dataframe
from Load_Covid_Data import send_files_to_oci
from config import var_dt_inicio, var_dt_fim, var_max_treading_data_sus

def exec_mult_treading(str_dt_inicio, str_dt_fim):
    download_covid_data(str_dt_inicio, str_dt_fim)
    semaphore.release()

def get_vacina_covid():
    relative_dt_begin = datetime.strptime(var_dt_inicio, "%Y-%m-%d").date()
    relative_dt_end = relative_dt_begin + relativedelta(days=+1)

    threads = []
    while relative_dt_begin < datetime.strptime(var_dt_fim, "%Y-%m-%d").date():
        str_dt_inicio = str(relative_dt_begin)
        str_dt_fim = str(relative_dt_end)
        print("*************************************************************")
        print(f"{str_dt_inicio}")
        print(f"{str_dt_fim}")
        print("*************************************************************")

        relative_dt_begin = relative_dt_end
        relative_dt_end = relative_dt_end + relativedelta(days=+1)
        semaphore.acquire()
        thread = Thread(target=exec_mult_treading, args=(str_dt_inicio, str_dt_fim))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    semaphore = Semaphore(var_max_treading_data_sus)
    dh_inicio = datetime.now()
    get_vacina_covid()
    transformar_dataframe()
    send_files_to_oci()

    duracao = datetime.now()- dh_inicio
    print("#############################################################")
    print("FIM")
    print(f"Duração: {duracao}")
    print("#############################################################")
