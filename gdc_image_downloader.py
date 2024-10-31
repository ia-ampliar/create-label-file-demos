from typing import List, Dict
import os
import requests
from tqdm import tqdm  # For progress bar
import pandas as pd  # Para ler o CSV
from boto3 import client

# Constantes
GDC_BASE_URL = "https://api.gdc.cancer.gov"

def download_specific_cases_images(cases_codes: List[str], images_list: List[str]) -> None:
    for case in cases_codes:
        case_uuid = get_case_uuid(case)

        if case_uuid == "Caso não encontrado.":
            print(f"Case {case} not found, skipping.")
            continue

        case_files = get_case_files(case_uuid)

        for file in case_files:
            case_dir = f"./data/{case}"  # Diretório para salvar arquivos
            if file['file_name'] in images_list:
                download_file_with_resume(file_id=file['file_id'], 
                                          file_name=file['file_name'], 
                                          destination_folder=case_dir)

def get_case_uuid(case_code: str) -> str:
    case_url = f"{GDC_BASE_URL}/cases"
    params = {
        "filters": {
            "op": "in",
            "content": {
                "field": "submitter_id",
                "value": [case_code]
            }
        },
        "fields": "case_id,submitter_id",
        "format": "json",
        "size": "1"
    }

    try:
        response = requests.post(case_url, json=params)
        response.raise_for_status()
        data = response.json()
        if data['data']['hits']:
            return data['data']['hits'][0]['case_id']
        return "Caso não encontrado."

    except requests.RequestException as e:
        return f"Request failed: {e}"

def get_case_files(case_uuid: str) -> List[Dict[str, str]]:
    files_url = f"{GDC_BASE_URL}/files"
    params = {
        "filters": {
            "op": "and",
            "content": [
                {
                    "op": "in",
                    "content": {
                        "field": "cases.case_id",
                        "value": [case_uuid]
                    }
                }
            ]
        },
        "fields": "file_name,file_id",
        "format": "json",
        "size": "1000"
    }

    try:
        response = requests.post(files_url, json=params)
        response.raise_for_status()
        data = response.json()
        return [
            {'file_name': file['file_name'], 'file_id': file['file_id']}
            for file in data['data']['hits'] if file['file_name'].endswith('.svs')
        ]
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return []

def download_file_with_resume(file_id: str, file_name: str, destination_folder: str) -> None:
    download_url = f"{GDC_BASE_URL}/data/{file_id}"
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)
    
    file_path = os.path.join(destination_folder, file_name)
    headers = {}
    
    # Verificar se o download já começou
    if os.path.exists(file_path):
        downloaded_bytes = os.path.getsize(file_path)
        headers['Range'] = f'bytes={downloaded_bytes}-'
    else:
        downloaded_bytes = 0

    try:
        with requests.get(download_url, headers=headers, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0)) + downloaded_bytes
            with open(file_path, 'ab') as f, tqdm(
                total=total_size, initial=downloaded_bytes, unit='B', unit_scale=True
            ) as pbar:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
        print(f"{file_name} downloaded successfully.")

    except requests.RequestException as e:
        print(f"Failed to download {file_name}: {e}")

def read_test_list_from_csv(csv_path: str) -> List[str]:
    try:
        df = pd.read_csv(csv_path)
        return df['Image'].tolist()
    except FileNotFoundError:
        print(f"File {csv_path} not found.")
        return []
    except pd.errors.EmptyDataError:
        print(f"No data found in {csv_path}.")
        return []

def main():
    # Caminho do CSV contendo a lista de imagens
    csv_path = "test_list.csv"
    
    test_list = read_test_list_from_csv(csv_path)

    if not test_list:
        print("No images found to download. Exiting.")
        return

    download_list = list(set([item[:12] for item in test_list]))
    print(f'Number of cases: {len(download_list)}')
    print(f'Number of images: {len(test_list)}')

    download_specific_cases_images(download_list, test_list)

if __name__ == "__main__":
    main()
