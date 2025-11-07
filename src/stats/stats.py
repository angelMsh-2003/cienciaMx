import json
from datetime import datetime
from collections import Counter

class Stats (): 
    def __init__(self, json_file_path):
        self.json_file_path = json_file_path

    def load_data(self):
        with open(self.json_file_path, "r", encoding="utf-8") as f:
            return json.load(f)


    def get_oldest_item(self, data):
        """Devuelve el ítem más antiguo según el campo 'date'."""
        try:
            records = data.get("OAI-PMH", {}).get("ListRecords", {}).get("record", [])
            if not records:
                return None

            if isinstance(records, dict):
                records = [records]

            datestamps = []
            for rec in records:
                header = rec.get("header", {})
                datestamp = header.get("datestamp")
                if datestamp:
                    datestamps.append(datestamp)

            if not datestamps:
                return None

            # Convierte a objetos datetime para comparar
            parsed_dates = [datetime.strptime(ds, "%Y-%m-%dT%H:%M:%SZ") for ds in datestamps]
            oldest = min(parsed_dates)

            return oldest.strftime("%Y-%m-%dT%H:%M:%SZ")

        except Exception as e:
            print(f"Error en get_oldest_datestamp: {e}")
            return None


    def get_newest_item(self, data):
        """Devuelve el ítem más reciente según el campo 'date'."""
        try:
            records = data.get("OAI-PMH", {}).get("ListRecords", {}).get("record", [])
            if not records:
                return None

            # Asegura que records sea una lista
            if isinstance(records, dict):
                records = [records]

            datestamps = []
            for rec in records:
                header = rec.get("header", {})
                datestamp = header.get("datestamp")
                if datestamp:
                    datestamps.append(datestamp)

            if not datestamps:
                return None

            # Convierte a objetos datetime para comparar
            parsed_dates = [datetime.strptime(ds, "%Y-%m-%dT%H:%M:%SZ") for ds in datestamps]
            newest = max(parsed_dates)

            return newest.strftime("%Y-%m-%dT%H:%M:%SZ")

        except Exception as e:
            print(f"⚠️ Error en get_newest_datestamp: {e}")
            return None


    def get_item_language(self, data):
        """Devuelve un conteo de ítems por idioma."""    
        try:
            records = data.get("OAI-PMH", {}).get("ListRecords", {}).get("record", [])
            if not records:
                return {}

            # Asegura que siempre sea una lista
            if isinstance(records, dict):
                records = [records]

            languages = []

            for rec in records:
                metadata = rec.get("metadata", {})
                oai_dc = metadata.get("oai_dc:dc", {})

                # En algunos casos puede haber más de un idioma
                dc_language = oai_dc.get("dc:language")

                if isinstance(dc_language, list):
                    for lang in dc_language:
                        if isinstance(lang, str):
                            languages.append(lang.strip().lower())
                elif isinstance(dc_language, str):
                    languages.append(dc_language.strip().lower())

            # Cuenta cuántas veces aparece cada idioma
            language_counts = dict(Counter(languages))

            return language_counts

        except Exception as e:
            print(f"⚠️ Error en count_languages: {e}")
            return {}


    def display_data(self, data, n=5):
        """Muestra los primeros n elementos del dataset."""
        for i, item in enumerate(data[:n], start=1):
            print(f"{i}. {item}")


if __name__ == "__main__": 

    path = r"data\Repositorio_Institucional_UnADM.json"

    st = Stats (path)

    data = st.load_data ()
    
    print (f"Fecha mas vieja: {st.get_oldest_item (data)}")
    print (f"Fecha mas nueva: {st.get_newest_item (data)}")
    print (f"Idiomas: {st.get_item_language (data)}")