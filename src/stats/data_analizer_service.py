import json
import csv
import os
from datetime import datetime
from collections import Counter

class DataAnalizerService:
    DUBLIN_CORE_FIELDS = [
        "dc:title", "dc:creator", "dc:subject", "dc:description",
        "dc:publisher", "dc:contributor", "dc:date", "dc:type",
        "dc:format", "dc:identifier", "dc:source", "dc:language",
        "dc:relation", "dc:coverage", "dc:rights"
    ]

    def __init__(self, json_folder, required_fields, mandatory_fields, output_csv):
        self.json_folder = json_folder
        self.required_fields = required_fields
        self.mandatory_fields = mandatory_fields
        self.output_csv = output_csv

    # -------------------- CORE LOADERS --------------------
    def load_data(self, json_path):
        """Carga un archivo JSON y lo devuelve como dict."""
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def get_records(self, data):
        """Extrae la lista de records del JSON."""
        records = data.get('OAI-PMH', {}).get('ListRecords', {}).get('record', [])
        if isinstance(records, dict):
            records = [records]
        return records

    # -------------------- VALIDACIONES --------------------
    def validate_record(self, record):
        """Valida campos obligatorios dentro del registro."""
        metadata = (record.get('metadata') or {}).get('oai_dc:dc') or {}
        errors = []
        missing_fields = []

        for field in self.mandatory_fields:
            if field not in metadata:
                errors.append(f"Missing {field}")
                missing_fields.append(field)

        record_id = metadata.get('dc:identifier', record.get('header', {}).get('identifier', 'no_id'))
        return errors, missing_fields, record_id, metadata.keys()

    # -------------------- MÉTODOS DE ESTADÍSTICAS --------------------
    def get_oldest_item(self, records):
        datestamps = [
            r.get("header", {}).get("datestamp")
            for r in records if r.get("header", {}).get("datestamp")
        ]
        if not datestamps:
            return ""
        parsed = [datetime.strptime(d, "%Y-%m-%dT%H:%M:%SZ") for d in datestamps]
        return min(parsed).strftime("%Y-%m-%dT%H:%M:%SZ")

    def get_newest_item(self, records):
        datestamps = [
            r.get("header", {}).get("datestamp")
            for r in records if r.get("header", {}).get("datestamp")
        ]
        if not datestamps:
            return ""
        parsed = [datetime.strptime(d, "%Y-%m-%dT%H:%M:%SZ") for d in datestamps]
        return max(parsed).strftime("%Y-%m-%dT%H:%M:%SZ")

    def get_language_summary(self, records):
        languages = []
        for r in records:
            oai_dc = (r.get("metadata") or {}).get("oai_dc:dc") or {}
            dc_lang = oai_dc.get("dc:language")
            if isinstance(dc_lang, list):
                languages.extend([lang.strip().lower() for lang in dc_lang if isinstance(lang, str)])
            elif isinstance(dc_lang, str):
                languages.append(dc_lang.strip().lower())
        counts = dict(Counter(languages))
        # Devuelve como JSON string escapado para que salga como en tu ejemplo
        return json.dumps(counts, ensure_ascii=False)

    def get_info_items(self, records):
        deleted_records = 0
        records_with_errors = 0
        records_complete = 0
        all_fields = set()
        missing_fields_info = []

        for record in records:
            header = record.get("header", {})
            if header.get("@status") == "deleted":
                deleted_records += 1
                continue

            errors, missing_fields, record_id, fields = self.validate_record(record)
            all_fields.update(fields)

            if errors:
                records_with_errors += 1
                missing_fields_info.append(f"{record_id}: {', '.join(missing_fields)}")
            else:
                records_complete += 1

        return {
            "deleted_records": deleted_records,
            "records_with_errors": records_with_errors,
            "records_complete": records_complete,
            "metadata_fields": ", ".join(sorted(all_fields)),
            "mandatory_fields": " | ".join(missing_fields_info) if missing_fields_info else "All mandatory fields present"
        }
    
    def get_dublin_core_completeness(self, items):
        """
        Analiza el porcentaje de cumplimiento de etiquetas Dublin Core
        por repositorio (archivo JSON completo).
        """
        if isinstance(items, dict):
            items = [items]

        # Excluir los ítems que tengan status 'deleted'
        active_items = []
        for item in items:
            header = item.get("header", {})
            status = header.get("@status")
            if status != "deleted":
                active_items.append(item)

        total_items = len(active_items)
        if total_items == 0:
            return {field: 0 for field in self.DUBLIN_CORE_FIELDS}

        field_counts = {field: 0 for field in self.DUBLIN_CORE_FIELDS}

        for item in items:
            dc_section = item.get("metadata", {}).get("oai_dc:dc", {})
            for field in self.DUBLIN_CORE_FIELDS:
                value = dc_section.get(field)

                # Verificar existencia y contenido válido
                # print (f"dublin {field} and Value {value}\n")
                if value:
                    if isinstance(value, str):
                        if value.strip() not in ["", "None"]:
                            field_counts[field] += 1
                    elif isinstance(value, list):
                        if any(str(v).strip() not in ["", "None"] for v in value):
                            field_counts[field] += 1
                    elif isinstance(value, dict):
                        text_val = value.get("#text")
                        if text_val and text_val.strip() not in ["", "None"]:
                            field_counts[field] += 1
        percentages = {
            field: round((count / total_items) * 100, 2)
            for field, count in field_counts.items()
        }

        return percentages

    # -------------------- EJECUCIÓN PRINCIPAL --------------------
    def run_stats(self, json_path):
        """Ejecuta las estadísticas seleccionadas dinámicamente."""
        data = self.load_data(json_path)
        records = self.get_records(data)
        if not records:
            return None

        total_records = len(records)
        results = {
            "json_file": os.path.basename(json_path),
            "total_records": total_records,
            "oldest_item": "",
            "newest_item": "",
            "language_summary": "",
            "deleted_records": 0,
            "records_with_errors": 0,
            "records_complete": 0,
            "metadata_fields": "",
            "mandatory_fields": "", 
            "dc:title" : 0,
            "dc:creator" : 0,
            "dc:subject" : 0,
            "dc:description" : 0,
            "dc:publisher" : 0,
            "dc:contributor" : 0,
            "dc:date" : 0,
            "dc:type" : 0,
            "dc:format" : 0,
            "dc:identifier" : 0,
            "dc:source" : 0,
            "dc:language" : 0,
            "dc:relation" : 0,
            "dc:coverage" : 0,
            "dc:rights" : 0
        }

        for field in self.required_fields:
            method_name = f"get_{field}"
            method = getattr(self, method_name, None)

            if callable(method):
                result = method(records)
                if isinstance(result, dict):
                    results.update(result)
                else:
                    results[field] = result
            else:
                results[field] = f"[Warning] Método '{method_name}' no encontrado"

        return results

    def search_json(self):
        """Itera sobre todos los JSON del folder y genera CSV."""
        combined_rows = []
        for filename in os.listdir(self.json_folder):
            if filename.endswith(".json"):
                json_path = os.path.join(self.json_folder, filename)
                result = self.run_stats(json_path)
                if result:
                    combined_rows.append(result)
        self.save_csv_result(combined_rows)

    def save_csv_result(self, combined_rows):
        """Guarda los resultados en CSV en el orden exacto solicitado."""
        if not combined_rows:
            print("No se generaron resultados.")
            return

        fieldnames = [
            "json_file",
            "oldest_item",
            "newest_item",
            "language_summary",
            "total_records",
            "deleted_records",
            "records_with_errors",
            "records_complete",
            "metadata_fields",
            "mandatory_fields",
            "dc:title" ,
            "dc:creator" ,
            "dc:subject" ,
            "dc:description" ,
            "dc:publisher" ,
            "dc:contributor" ,
            "dc:date" ,
            "dc:type" ,
            "dc:format" ,
            "dc:identifier" ,
            "dc:source" ,
            "dc:language" ,
            "dc:relation" ,
            "dc:coverage" ,
            "dc:rights",
        ]

        with open(self.output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(combined_rows)

        print(f"CSV generado correctamente: {self.output_csv}")


# -------------------- MAIN --------------------
if __name__ == "__main__":
    mandatory_fields = ['dc:title', 'dc:creator', 'dc:rights', 'dc:date', 'dc:type', 'dc:identifier']
    json_folder = "data"
    output_csv = "csv/MetaValidatorReport.csv"

    required_fields = ['oldest_item', 'newest_item', 'language_summary', 'info_items', 'dublin_core_completeness']

    analyzer = DataAnalizerService(
        json_folder=json_folder,
        required_fields=required_fields,
        mandatory_fields=mandatory_fields,
        output_csv=output_csv
    )

    analyzer.search_json()
