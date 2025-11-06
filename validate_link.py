import requests
import xml.etree.ElementTree as ET 

link = "http://rdu.iquimica.unam.mx/oai/public?"
# verb=GetRecord&identifier=----&metadataPrefix=oai_dc
# rdu.iquimica.unam.mx/handle/20.500.12214/679
param_getrecord = {
    'verb' : 'GetRecord', 
    'identifier' : 'oai:bibliotecavirtual.dgb.umich.mx:DGB_UMICH/3593', 
    'metadataPrefix' : 'oai_dc'
}
param_listrecord = {
    'verb' : 'ListRecords', 
    'metadataPrefix' : 'oai_dc'
}

param_ListIdentifiers = {
    'verb' : 'ListIdentifiers', 
    'metadataPrefix' : 'oai_dc'
}
def validate (): 
    try: 
        result = requests.get (link, timeout=(60), params=param_getrecord)
        print (f"Status {result.status_code}")
        if result.status_code == 200: 
            datos = result.text
            print (datos)

        
            # raiz = ET.fromstring (datos, )
            # ns = {'oai': 'http://www.openarchives.org/OAI/2.0/'}
    except Exception as e: 
        print (f"Error: {e}")

def find_error (): 
    try: 
        result = requests.get (link, timeout=(60), params=param_getrecord)
        if result.status_code == 200: 
            data = result.text
            root = ET.fromstring (data)
            ns = {'oai': "http://www.openarchives.org/OAI/2.0/"}

            error_elem = root.find ('.//oai:error', ns)

            if error_elem is not None: 
                error_code = error_elem.attrib.get("code", "unknown")
                error_message = error_elem.text.strip() if error_elem.text else ""
                print("⚠️ Ocurrió un error OAI-PMH:")
                print(f"  Código: {error_code}")
                print(f"  Mensaje: {error_message}")

            else: 
                print ("no existen errores")

    except Exception as e: 
        print (f"Error: {e}")

validate ()