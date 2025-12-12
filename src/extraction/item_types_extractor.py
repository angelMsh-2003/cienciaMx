import json
import os
import glob


def extraer_tipos_unicos(directorio_entrada, archivo_salida):
    # Usamos un set (conjunto) para guardar los tipos,
    # ya que los sets eliminan duplicados automáticamente.
    tipos_unicos = set()

    # Busca todos los archivos .json en el directorio indicado
    patron = os.path.join(directorio_entrada, '*.json')
    archivos = glob.glob(patron)

    print(f"Encontrados {len(archivos)} archivos JSON. Procesando...")

    for archivo in archivos:
        try:
            with open(archivo, 'r', encoding='utf-8') as f:
                contenido = json.load(f)

                # Verificamos que el contenido sea una lista (array de objetos)
                if isinstance(contenido, list):
                    for registro in contenido:
                        # Obtenemos el valor de 'type', devuelve None si no existe
                        valor_type = registro.get('type')

                        if valor_type:
                            # CASO 1: Es una lista (el caso raro que mencionaste)
                            if isinstance(valor_type, list):
                                for sub_tipo in valor_type:
                                    if sub_tipo:  # Evitar guardar valores vacíos
                                        tipos_unicos.add(str(sub_tipo).strip())

                            # CASO 2: Es un string normal
                            elif isinstance(valor_type, str):
                                tipos_unicos.add(valor_type.strip())

        except json.JSONDecodeError:
            print(f"Advertencia: El archivo {archivo} no tiene un formato JSON válido y fue omitido.")
        except Exception as e:
            print(f"Error procesando {archivo}: {e}")

    # Guardar los resultados en el archivo de texto
    try:
        with open(archivo_salida, 'w', encoding='utf-8') as f_out:
            # Ordenamos la lista alfabéticamente para que sea más fácil de leer
            for tipo in sorted(tipos_unicos):
                f_out.write(f"{tipo}\n")

        print(f"¡Listo! Se han guardado {len(tipos_unicos)} tipos únicos en '{archivo_salida}'.")

    except Exception as e:
        print(f"Error al escribir el archivo de salida: {e}")


# --- CONFIGURACIÓN ---
# Pon aquí la ruta de tu carpeta con los JSONs.
# '.' significa la carpeta actual donde ejecutes el script.
INPUT_DIR = r"../../data/input/normalized/"

# Nombre del archivo resultante
nombre_archivo_salida = 'types.txt'

# Ejecutar la función
if __name__ == "__main__":
    # Asegúrate de crear la carpeta de prueba o cambiar la ruta
    if not os.path.exists(INPUT_DIR):
        print(f"Error: La carpeta '{INPUT_DIR}' no existe. Por favor ajusta la variable 'carpeta_con_jsons'.")
    else:
        extraer_tipos_unicos(INPUT_DIR, nombre_archivo_salida)