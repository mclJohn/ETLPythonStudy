import os
import requests
import json
import pandas as pd

#extract ---------------------------------
def extract_data(endpoint):
    print(endpoint)
    response = requests.get(endpoint)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"erro ao extrair dados da api:{response.status_code}")
        return None
#extract ---------------------------------        

#load --------------------------------- 

def load_data(data, path):
    os.makedirs(path, exist_ok=True)  # cria a pasta se não existir
    item_id = data.get("id")  # pega o campo 'id' do JSON
    if item_id is None:
        print("⚠️ ID não encontrado nos dados.")
        return

    with open(f"{path}/{item_id}.json", "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, ensure_ascii=False)
        print(f"✅ Dados salvos em: {path}/{item_id}.json")

def loop_load_data(endpoint):
    url = f"https://dummyjson.com/{endpoint}"
    i = 1
    while i <= 50:
        data = extract_data(f"{url}/{i}")
        if data:
            load_data(data, f"data/{endpoint}")
            i += 1
        else:
            break

#load --------------------------------- 


#transform --------------------------------- 

def transform_data_json_to_csv(endpoint, index):
    import itertools

    print(f"Transformando raw/{endpoint}/{index}.json → curated/{endpoint}/{index}.csv")
    
    os.makedirs(f"curated/{endpoint}", exist_ok=True)

    with open(f"raw/{endpoint}/{index}.json", "r") as file:
        data = json.load(file)

    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, dict):
        # Verifica se todos os valores são listas
        if all(isinstance(v, list) for v in data.values()):
            # Tenta alinhar os dados pelo menor tamanho comum
            min_length = min(len(v) for v in data.values())
            rows = []
            for i in range(min_length):
                row = {key: data[key][i] for key in data}
                rows.append(row)
            df = pd.DataFrame(rows)
        else:
            
            df = pd.DataFrame([data])
    else:
        print(f"Formato inesperado no JSON: {type(data)}")
        return

    df.to_csv(f"curated/{endpoint}/{index}.csv", index=False)

endpoints = ["user", "products"]

for endpoint in endpoints:
    for i in range(1, 10 + 1):
        transform_data_json_to_csv(endpoint, i)
        
#transform --------------------------------- 




#.\venv\Scripts\Activate
# pip install requests
# python etl.py
# pip install pandas

