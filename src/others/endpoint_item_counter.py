import requests
import xml.etree.ElementTree as ET 
import pandas as pd
import random
import sys
import time

class Endpoint_item_counter(): 
    def __init__(self, csv_input_path, csv_output_path, parameters):
        self.csv_input_path = csv_input_path
        self.csv_output_path = csv_output_path
        self.parameters = parameters

    def search_items(self): 
        df = pd.read_csv(self.csv_input_path)

        for index, row in df.iterrows(): 
            try: 
                repo_name = row['name']
                link = row['link']

                print(f"\n------ REPO {index}: {repo_name} ------")

                identifiers = self.find_ListRecords(link)

                if identifiers is not None: 
                    num_items = int(len(identifiers))
                    print(f"- Número total de elementos: {num_items}") 
                    df.loc[index, 'num_items'] = num_items
                else:
                    df.loc[index, 'num_items'] = None

            except Exception as e: 
                print(f"Error procesando {repo_name}: {e}")
                df.loc[index, 'num_items'] = None

        df.to_csv (self.csv_output_path, index=False)
        print("\n✅ Archivo CSV actualizado correctamente.")

    def find_ListRecords(self, link): 
        identifiers = []
        ns = {'oai': 'http://www.openarchives.org/OAI/2.0/'}

        try:
            # Primera petición con los parámetros iniciales
            response = requests.get(link, timeout=(60), params=self.parameters)
            response.raise_for_status()

            while True:
                datos = response.text
                raiz = ET.fromstring(datos)

                # Extrae los identificadores
                current_ids = [elem.text for elem in raiz.findall('.//oai:identifier', ns)]
                identifiers.extend(current_ids)

                # Verifica si hay un resumptionToken
                token_elem = raiz.find('.//oai:resumptionToken', ns)
                if token_elem is not None and token_elem.text:
                    resumption_token = token_elem.text.strip()
                    print(f"➡️  Resumption token encontrado: {resumption_token}")
                    
                    # Nueva solicitud usando el token
                    next_params = {'verb': 'ListRecords', 'resumptionToken': resumption_token}
                    time.sleep(1)  # Evita sobrecargar el servidor
                    response = requests.get(link, timeout=(60), params=next_params)
                    response.raise_for_status()
                else:
                    break  # No hay más páginas, termina el bucle

            return identifiers

        except Exception as e:
            print(f"⚠️  Error en find_ListRecords: {e}")
            return None

            

if __name__ == "__main__": 

    # PATHS INPUT
    csv_input_path = r'csv\data.csv'
    csv_output_path = r'csv\result.csv'

    parameters = {
        'verb' : 'ListIdentifiers', 
        'metadataPrefix' : 'oai_dc'
    }

    # Defining our object
    item_counter = Endpoint_item_counter (
        csv_input_path=csv_input_path, 
        csv_output_path=csv_output_path,
        parameters=parameters
    )
    
    item_counter.search_items ()