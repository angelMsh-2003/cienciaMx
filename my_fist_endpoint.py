import requests
import json 

#definir el endpoint 
url = "https://httpbin.org/get"

print (f"llamando a la api {url}")

mis_headers = {
    "Authorization": "Bearer MI_API_KEY_SECRETA_12345",
    "User-Agent": "MiScriptDePython/1.0",
    "X-Mi-Header-Personalizado": "Hola"
}
try:
    # Pasamos el diccionario al parámetro params 
    # result = requests.get (url, params=mis_parametros)
    result = requests.get(url, headers=mis_headers)

    if result.status_code == 200: 
        datos = result.json ()

        print("\nHeaders recibidos por la API (extracto):")
        print(f"Authorization: {datos['headers']['Authorization']}")
        print(f"User-Agent: {datos['headers']['User-Agent']}")
        print(f"X-Mi-Header-Personalizado: {datos['headers']['X-Mi-Header-Personalizado']}")
        
        print (f"\nJson completo {json.dumps(datos, indent=4)}")

except Exception as e: 
    print (f"Algo fallo: {e}")