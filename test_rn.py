import requests
import xml.etree.ElementTree as ET 
import pandas as pd
import random
import sys
#definir el endpoint https://tesis.ipn.mx/oai/request?verb=ListRecords&metadataPrefix=oai_dc&set=col_123456789_19115
# url = "https://infotec.repositorioinstitucional.mx/oai/request"
# verb=GetRecord&identifier=----&metadataPrefix=oai_dc

# ?verb=GetRecord&identifier=
original_stdout = sys.stdout
output_csv_path = r"csv\result.csv"
path_csv = r"csv\data.csv"
path_output_txt = r"terminal_output.txt"
param_getRecords = {
    'verb' : 'GetRecord', 
    'metadataPrefix' : 'oai_dc'
}

param_listRecords = {
    'verb' : 'ListRecords', 
    'metadataPrefix' : 'oai_dc'
}

output_csv = {
    'repo_name' : [], 
    'link' : [], 
    'id_oai' : [], 
    'status_code' : [],
    'internal_error' : [],
    'messege' : []
}


def iterate_csv ():     
    total_id = 0.0
    with open (path_output_txt, 'w', encoding='utf-8') as f: 
        df = pd.read_csv (path_csv)

        for indice, fila in df.iterrows (): 
            repo_name = fila ['repo_name']
            link = fila ['link']

            print (f"\n------REPO {indice}: {repo_name}", file=sys.stdout)
            print (f"\n------REPO {indice}: {repo_name}", file=f)

            identifiers = find_ListRecords (f, link)

            if identifiers is not None: 
                # num_samples = calculate_sample (len(identifiers))
                print (f"- Numero de elementos {len(identifiers)}", file=sys.stdout)
                print (f"- Numero de elementos {len(identifiers)}", file=f)

                print ("Resultados: ", file=sys.stdout)
                print ("Resultados: ", file=f)
                #valid_status (f, link, identifiers, repo_name)--------
                total_id += int(len(identifiers))
            else :
                output_csv ['repo_name'].append (repo_name)
                output_csv ['link'].append (link)
                output_csv ['id_oai'].append ('NOT FOUND') 
                output_csv ['status_code'].append ('NOT FOUND')
                output_csv ['internal_error'].append ('NOT FOUND')
                output_csv ['messege'].append ('NOT FOUND')
                print ("Error en el repositorio", file=sys.stdout)
                print ("Error en el repositorio", file=f)

    df = pd.DataFrame (output_csv)
    df.to_csv (output_csv_path)
    print (f"Diccionario Guardado en {output_csv_path}")

    print (f"total_id : {total_id}")
    
def find_ListRecords (f, link): 
    try: 
        result = requests.get (link, timeout=(10), params=param_listRecords)
        if result.status_code == 200:   
            datos = result.text
            raiz = ET.fromstring (datos)
            ns = {'oai': 'http://www.openarchives.org/OAI/2.0/'}
            identifiers = [elem.text for elem in raiz.findall('.//oai:identifier', ns)]
        return identifiers
    except Exception as e: 
        print (f"Algo FALLO en find_listRecords", file=sys.stdout)
        print (f"Algo FALLO en find_listRecords", file=f)
        return None
        

def valid_status (f, link, identifiers, repo_name,num_samples=10): 
    ready_samples = []
    error_code = "NaN"
    error_message = "NaN"
    if num_samples > len(identifiers):
        num_samples = len(identifiers)-1
        print(f"Ajustando num_samples a {num_samples} (tam maximo posible)", file=f)
        print(f"Ajustando num_samples a {num_samples} (tam maximo posible)", file=sys.stdout)
    for i in range (num_samples): 
        try:
            while True: 
                value = random.randint (0,len(identifiers)-1) 
                if value not in ready_samples: 
                    ready_samples.append(value)
                    break

            id_oai = identifiers [value]
            param_getRecords ['identifier'] = id_oai

            output_csv ['repo_name'].append (repo_name)
            output_csv ['link'].append (link)
            output_csv ['id_oai'].append (id_oai)

            result = requests.get (link, timeout=(60), params=param_getRecords)
            
            if result.status_code == 200: 
                print (f"OK -> ID {id_oai}", file=sys.stdout)
                print (f"OK -> ID {id_oai}", file=f)

                data = result.text 
                root = ET.fromstring (data)
                ns = {'oai': "http://www.openarchives.org/OAI/2.0/"}

                error_elem = root.find ('.//oai:error', ns)

                if error_elem is not None: 
                    error_code = error_elem.attrib.get("code", "unknown")
                    error_message = error_elem.text.strip() if error_elem.text else "NaN" 
            else : 
                print (f"Failed -> ID {id_oai}", file=sys.stdout)
                print (f"Failed -> ID {id_oai}", file=f)

            output_csv ['status_code'].append (result.status_code)
            output_csv ['internal_error'].append (error_code)
            output_csv ['messege'].append (error_message)
            
        except Exception as e: 
            print (f"Algo fallo: {e}", file=sys.stdout)
            print (f"Algo fallo: {e}", file=f)

            output_csv ['status_code'].append ('ERROR')
            output_csv ['internal_error'].append ('ERROR')
            output_csv ['messege'].append (e)
            continue

iterate_csv ()