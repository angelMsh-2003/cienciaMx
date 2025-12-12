import json
import os

# --- DICCIONARIO DE NORMALIZACIÓN ---
NORMALIZE_TYPE = {
    # --- TESIS Y TRABAJOS DE GRADO ---
    "DOCTORALTHESIS": "Tesis de doctorado",
    "DOCTORALDEGREEWORK": "Trabajo de grado, doctorado",
    "MASTERTHESIS": "Tesis de maestría",
    "MASTERDEGREEWORK": "Trabajo de grado, maestría",
    "BACHELORTHESIS": "Tesis de licenciatura",
    "BACHELORDEGREEWORK": "Trabajo de grado, licenciatura",
    "ACADEMICSPECIALIZATION": "Trabajo terminal, especialidad",
    "THESIS": "Tesis",

    # --- ARTÍCULOS Y PUBLICACIONES PERIÓDICAS ---
    "ARTICLE": "Artículo",
    "CONTRIBUTIONTOPERIODICAL": "Contribución a publicación periódica",
    "REVIEW": "Reseña crítica",
    "PREPRINT": "Preimpreso",

    # --- LIBROS Y CAPÍTULOS ---
    "BOOK": "Libro",
    "EBOOK": "Libro",
    "BOOKPART": "Capítulo de libro",
    "BOOK CHAPTER": "Capítulo de libro",

    # --- CONGRESOS, CONFERENCIAS Y EVENTOS ---
    "CONFERENCEOBJECT": "Objeto de congreso",
    "CONFERENCECONTRIBUTION": "Objeto de congreso no publicado",
    "CONFERENCEPAPER": "Ítem publicado en memoria de congreso",
    "CONFERENCEPROCEEDINGS": "Memoria de congreso",
    "CONFERENCEPOSTER": "Póster de congreso",
    "LECTURE": "Conferencia",
    "PRESENTATION": "Conferencia",
    "EVENT": "Evento",

    # --- REPORTES Y DOCUMENTACIÓN TÉCNICA ---
    "REPORT": "Reporte",
    "TECHNICAL REPORT": "Reporte",
    "REPORTPART": "Parte de reporte",
    "TECHNICALDOCUMENTATION": "Documentación técnica",
    "WORKINGPAPER": "Documento de trabajo",
    "RESEARCHPROPOSAL": "Protocolo de investigación",
    "PATENT": "Patente",

    # --- MULTIMEDIA Y OTROS OBJETOS ---
    "IMAGE": "Imagen",
    "VIDEO": "Video",
    "MAP": "Mapa",
    "LEARNING OBJECT": "Objeto de aprendizaje",
    "ANNOTATION": "Anotación",
    "OTHER": "Otros"
}


def load_json(ruta_archivo):
    try:
        with open(ruta_archivo, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError:
        print(f"Error: '{ruta_archivo}' no tiene un formato JSON válido.")
        return None
    except Exception as e:
        print(f"Error inesperado al leer '{ruta_archivo}': {e}")
        return None


def update_repository_name(datos):
    """
    Función interactiva:
    1. Lee el 'repository' del primer registro.
    2. Pregunta al usuario si desea cambiarlo.
    3. Si 's', pide el nuevo nombre y actualiza todos los registros.
    """
    cambios = 0
    # Aseguramos que sea lista y tenga datos
    lista_datos = datos if isinstance(datos, list) else [datos]

    if not lista_datos:
        return lista_datos, 0

    # Obtenemos el valor actual (asumiendo que es uniforme)
    valor_actual = lista_datos[0].get("repository", "Sin Nombre")

    print(f"\n>> INTERACCIÓN REQUERIDA:")
    while True:
        respuesta = input(f"¿Desea cambiar el nombre a '{valor_actual}'? (s / n): ").lower().strip()
        if respuesta in ['s', 'n']:
            break
        print("Por favor ingresa 's' para sí o 'n' para no.")

    if respuesta == 's':
        nuevo_nombre = input("Escribir nuevo nombre: ").strip()
        # Iteramos y actualizamos
        for registro in lista_datos:
            # Solo actualizamos si realmente es diferente para contar el cambio correctamente
            if registro.get('repository') != nuevo_nombre:
                registro['repository'] = nuevo_nombre
                cambios += 1

    return lista_datos, cambios


def clean_invalid_identifiers(datos):
    registros_validos = []
    registros_eliminados = 0

    lista_datos = datos if isinstance(datos, list) else [datos]

    for registro in lista_datos:
        identifiers = registro.get('identifier')

        if not isinstance(identifiers, list):
            registros_eliminados += 1
            continue

        identifiers_limpios = [
            url.strip() for url in identifiers
            if isinstance(url, str) and url.strip()
        ]

        if len(identifiers_limpios) > 0:
            registro['identifier'] = identifiers_limpios
            registros_validos.append(registro)
        else:
            registros_eliminados += 1

    return registros_validos, registros_eliminados, 0


def process_titles(datos):
    registros_validos = []
    registros_eliminados = 0
    titulos_corregidos = 0

    lista_datos = datos if isinstance(datos, list) else [datos]

    for registro in lista_datos:
        title = registro.get('title')
        conservar = False
        nuevo_titulo = None

        if isinstance(title, list):
            for item in title:
                if isinstance(item, str) and len(item.strip()) > 0:
                    nuevo_titulo = item.strip()
                    conservar = True
                    break
            if conservar:
                registro['title'] = nuevo_titulo
                titulos_corregidos += 1

        elif isinstance(title, str):
            if len(title.strip()) > 0:
                registro['title'] = title.strip()
                conservar = True
            else:
                conservar = False

        elif title is None:
            conservar = False
        else:
            conservar = True

        if conservar:
            registros_validos.append(registro)
        else:
            registros_eliminados += 1

    return registros_validos, registros_eliminados, titulos_corregidos


def clean_subjects(datos):
    elementos_modificados = 0
    lista_datos = datos if isinstance(datos, list) else [datos]

    for registro in lista_datos:
        if 'subject' in registro and isinstance(registro['subject'], list):
            original_len = len(registro['subject'])
            registro['subject'] = [
                tema for tema in registro['subject']
                if not tema.strip().isdigit()
            ]
            if len(registro['subject']) < original_len:
                elementos_modificados += 1

    return lista_datos, elementos_modificados


def normalize_types(datos):
    registros_modificados = 0
    lista_datos = datos if isinstance(datos, list) else [datos]

    for registro in lista_datos:
        raw_type = registro.get('type')
        final_type_str = None
        hubo_cambio = False

        if isinstance(raw_type, list):
            if len(raw_type) > 0 and isinstance(raw_type[0], str):
                final_type_str = raw_type[0]
                hubo_cambio = True
            else:
                final_type_str = None
        elif isinstance(raw_type, str):
            final_type_str = raw_type

        if final_type_str:
            key_upper = final_type_str.strip().upper()

            if key_upper in NORMALIZE_TYPE:
                valor_traducido = NORMALIZE_TYPE[key_upper]
                if valor_traducido != raw_type:
                    registro['type'] = valor_traducido
                    registros_modificados += 1

            elif hubo_cambio:
                registro['type'] = final_type_str
                registros_modificados += 1

    return lista_datos, registros_modificados


def save_normalized_data(ruta_archivo, datos):
    try:
        with open(ruta_archivo, 'w', encoding='utf-8') as f:
            json.dump(datos, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Error al guardar '{ruta_archivo}': {e}")


def process_directory(carpeta_origen, carpeta_destino):
    if not os.path.exists(carpeta_origen):
        print(f"Error: La carpeta de origen '{carpeta_origen}' no existe.")
        return

    if not os.path.exists(carpeta_destino):
        os.makedirs(carpeta_destino)
        print(f"Carpeta creada: {carpeta_destino}")

    archivos = os.listdir(carpeta_origen)
    archivos_json = [f for f in archivos if f.lower().endswith('.json')]

    print(f"Encontrados {len(archivos_json)} archivos JSON. Iniciando proceso...\n")

    count_archivos_procesados = 0

    for nombre_archivo in archivos_json:
        ruta_entrada = os.path.join(carpeta_origen, nombre_archivo)
        ruta_salida = os.path.join(carpeta_destino, nombre_archivo)

        datos = load_json(ruta_entrada)

        if datos is not None:
            # --- NUEVO: Conteo inicial ---
            total_items = len(datos) if isinstance(datos, list) else 1
            print(f"► Procesando archivo: {nombre_archivo} (Items encontrados: {total_items})")

            # --- PASO 0: CAMBIO DE NOMBRE DE REPOSITORIO (INTERACTIVO) ---
            # Se ejecuta al inicio para trabajar sobre el nombre correcto
            datos, repo_changes = update_repository_name(datos)

            # 1. Títulos
            datos_step1, del_titles, fix_titles = process_titles(datos)

            # 2. Identifiers
            datos_step2, del_ids, _ = clean_invalid_identifiers(datos_step1)

            # 3. Tipos
            datos_step3, mod_types = normalize_types(datos_step2)

            # 4. Subjects
            datos_finales, mod_subjects = clean_subjects(datos_step3)

            # Guardar
            save_normalized_data(ruta_salida, datos_finales)

            # Reporte
            cambios = del_titles > 0 or fix_titles > 0 or mod_subjects > 0 or mod_types > 0 or del_ids > 0 or repo_changes > 0
            if cambios:
                print(f"   ✔ Resumen de cambios:")
                if repo_changes > 0: print(f"     - Repositorio renombrado en: {repo_changes} registros")
                if del_titles > 0: print(f"     - Borrados (título inválido): {del_titles}")
                if fix_titles > 0: print(f"     - Corregidos (título array->str): {fix_titles}")
                if del_ids > 0:    print(f"     - Borrados (identifier vacío): {del_ids}")
                if mod_types > 0:  print(f"     - Tipos normalizados: {mod_types}")
                if mod_subjects > 0: print(f"     - Subjects limpios: {mod_subjects}")
            else:
                print(f"   • Sin cambios necesarios.")
            print("-" * 50)

            count_archivos_procesados += 1

    print(f"\nProceso global finalizado. {count_archivos_procesados} archivos procesados.")


# --- Bloque Principal ---
def main():
    INPUT_DIR = r"../../data/input/normalized/"
    OUTPUT_DIR = r"../../data/output/normalized/"
    process_directory(INPUT_DIR, OUTPUT_DIR)


if __name__ == "__main__":
    main()