import oci
import os
from pathlib import Path
import threading
from config import oci_user, oci_fingerprint, oci_tenancy, oci_region, oci_bucketname
from Error_Log import write_erro_log

txt_lock = threading.Lock()

# Configuração do cliente OCI
def get_OCI_config():
    config = {
        "user": oci_user,
        "key_file": f"{Path(__file__).resolve().parent}\\Credentials_Wallet\\credentials_key.pem",
        "fingerprint": oci_fingerprint,
        "tenancy": oci_tenancy,
        "region": oci_region
    }
    return config

def oci_list_objects(oci_prefix=None):
    try:
        # Criar cliente
        object_storage = oci.object_storage.ObjectStorageClient(get_OCI_config())
        # Nome do namespace
        namespace = object_storage.get_namespace().data

        list_objects_response = object_storage.list_objects(namespace, oci_bucketname, prefix=oci_prefix)
        objects = list_objects_response.data.objects
        return objects

    except Exception as e:
        print("Error while listing objects:", e)

def upload_file(local_file_path, object_name):
    try:
        # Criar cliente
        object_storage = oci.object_storage.ObjectStorageClient(get_OCI_config())
        # Nome do namespace
        namespace = object_storage.get_namespace().data
        
        print("-------------------------------------------------------------")
        print(f"Upload: {object_name}")
        print("-------------------------------------------------------------")
        with open(local_file_path, "rb") as f:
            object_storage.put_object(namespace, oci_bucketname, object_name, f)
        
        return 0
    except Exception as e:
        write_erro_log(str(e))
        return None

def oci_download_file(object_name, local_file_path):
    try:
        # Criar cliente
        object_storage = oci.object_storage.ObjectStorageClient(get_OCI_config())
        
        # Nome do namespace
        namespace = object_storage.get_namespace().data
        get_object_response = object_storage.get_object(namespace, oci_bucketname, object_name)
        with open(local_file_path, "wb") as f:
            for chunk in get_object_response.data.raw.stream(1024 * 1024, decode_content=False):
                f.write(chunk)
        
        return 0
    
    # except oci.exceptions.ServiceError as e:
    except Exception as e:
        print("Error while listing objects:", e)
        return None

def oci_delete_file(bucket_key):
    try:
        # Criar cliente
        object_storage = oci.object_storage.ObjectStorageClient(get_OCI_config())
        # Nome do namespace
        namespace = object_storage.get_namespace().data
        
        print("-------------------------------------------------------------")
        object_storage.delete_object(namespace, oci_bucketname, bucket_key)
        print(f"Arquivo {bucket_key} excluído com sucesso do bucket {oci_bucketname}.")
        print("-------------------------------------------------------------")
        return 0
    except Exception as e:
        write_erro_log(str(e))
        return None
