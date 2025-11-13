import json
import os
import sys
import glob
from datetime import date
from urllib.parse import urlparse
from typing import Optional, List, Dict, Any, Tuple
from src.db.db_connector import get_db_connection
from src.db.repository_DTO import RepositoryDTO

try:
    from psycopg2.extras import execute_batch
except ImportError:
    print("Warning: psycopg2.extras not found. Batch insert may not be optimized.")

    def execute_batch(cursor, sql, argslist):
        for args in argslist:
            cursor.execute(sql, args)

try:
    from src.db.item_DTO import ItemDTO
    from src.db.repository_DTO import RepositoryDTO
    from src.db.db_connector import get_db_connection
except ImportError:
    print("Warning: Could not import DTOs or db_connector from 'src.db'.")

    # Mock classes for testing without dependencies
    class RepositoryDTO:
        def __init__(self, id, name, url, last_update, alive): pass

    class ItemDTO:
        def __init__(self, id, id_repository, title=None, creator=None, subject=None,
                     description=None, publisher=None, contributor=None, date=None,
                     type=None, format=None, identifier=None, source=None,
                     language=None, relation=None, coverage=None, rights=None):
            pass

    def get_db_connection():
        class MockConnection:
            def cursor(self): return MockCursor()
            def close(self): print("Mock connection closed.")
            def commit(self): print("Mock commit.")
            def rollback(self): print("Mock rollback.")

        class MockCursor:
            def __enter__(self): return self
            def __exit__(self, et, ev, tb): pass
            def execute(self, *args): pass
            def fetchone(self): return (1,)

        return MockConnection()


class InsertItemFromRepository:
    """ETL orchestrator for DSpace JSON exports"""

    def __init__(self):
        self.connection = get_db_connection()
        if not self.connection:
            raise ConnectionError("Failed to get database connection.")
        print("Database connection established.")

    def _extract_data_from_file(self, json_file_path: str) -> Tuple[Optional[List], Optional[str], Optional[str]]:
        """Extract records, URL and repository name from JSON file"""
        print(f"\n--- Reading file: {os.path.basename(json_file_path)} ---")
        
        if not os.path.exists(json_file_path):
            print(f"Error: File not found at {json_file_path}")
            return None, None, None
        
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"Error reading or parsing JSON: {e}")
            return None, None, None

        records_list: List[Dict[str, Any]] = data.get("OAI-PMH", {}).get("ListRecords", {}).get("record", [])
        if not records_list:
            print("Warning: No records found.")
        else:
            print(f"Successfully extracted {len(records_list)} records.")

        repo_url: Optional[str] = data.get("OAI-PMH", {}).get("request", {}).get("#text")
        repository_name: Optional[str] = None
        if repo_url:
            repository_name = urlparse(repo_url).netloc
            print(f"Extracted URL: {repo_url}")
            print(f"Extracted Repository Name: {repository_name}")
        else:
            print("Warning: Could not find repository URL.")

        return records_list, repo_url, repository_name

    def _find_or_create_repository(self, repo_dto: RepositoryDTO) -> Optional[int]:
        """Find or create repository, return ID"""
        print(f"Processing repository: {repo_dto.name}...")
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "SELECT id FROM repositories WHERE name = %s;",
                    (repo_dto.name,)
                )
                result = cursor.fetchone()

                repo_id: Optional[int] = None

                if result:
                    repo_id = result[0]
                    print(f"Repository '{repo_dto.name}' found (ID: {repo_id}). Updating.")
                    cursor.execute(
                        """
                        UPDATE repositories
                        SET last_update = %s, alive = %s, url = %s
                        WHERE id = %s;
                        """,
                        (repo_dto.last_update, repo_dto.alive, repo_dto.url, repo_id)
                    )
                else:
                    print(f"Repository '{repo_dto.name}' not found. Creating new entry.")
                    cursor.execute(
                        """
                        INSERT INTO repositories (name, url, last_update, alive)
                        VALUES (%s, %s, %s, %s)
                        RETURNING id; 
                        """,
                        (repo_dto.name, repo_dto.url, repo_dto.last_update, repo_dto.alive)
                    )
                    repo_id = cursor.fetchone()[0]
                    print(f"Repository created with ID: {repo_id}.")

                self.connection.commit()
                return repo_id

        except Exception as e:
            print(f"Database error processing repository '{repo_dto.name}': {e}", file=sys.stderr)
            self.connection.rollback()
            return None

    def _safe_extract(self, dc_data: Dict[str, Any], key: str) -> Optional[str]:
        """Extract and format metadata values handling lists and dicts"""
        value = dc_data.get(key)
        if not value:
            return None

        try:
            if isinstance(value, list):
                processed_items = []
                for item in value:
                    if isinstance(item, dict):
                        processed_items.append(item.get("#text", ""))
                    else:
                        processed_items.append(str(item))
                return "; ".join(filter(None, processed_items))
            if isinstance(value, dict):
                return value.get("#text")
            return str(value)
        except Exception as e:
            print(f"Warning: Could not parse key '{key}': {e}")
            return None

    def _extract_publication_date(self, dc_data: Dict[str, Any]) -> Optional[date]:
        """Parse publication date from dc:date, skip embargo dates"""
        date_list = dc_data.get("dc:date")
        if not isinstance(date_list, list):
            return None

        for date_str in date_list:
            # Skip embargo dates starting with info:eu-repo
            if date_str and not date_str.startswith("info:eu-repo"):
                try:
                    return date.fromisoformat(date_str)
                except (ValueError, TypeError):
                    # Handle year-only format
                    if len(date_str) == 4 and date_str.isdigit():
                        return date.fromisoformat(f"{date_str}-01-01")
                    continue
        return None

    def _get_existing_identifiers(self, repo_id: int) -> set:
        """Query existing item identifiers to prevent duplicates"""
        existing_identifiers = set()
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "SELECT identifier FROM items WHERE id_repository = %s AND identifier IS NOT NULL",
                    (repo_id,)
                )
                existing_identifiers = {row[0] for row in cursor.fetchall()}
            print(f"Found {len(existing_identifiers)} existing items with identifiers in DB")
        except Exception as e:
            print(f"Warning: Could not check existing items: {e}")
        return existing_identifiers

    def _map_record_to_dto(self, record: Dict, repo_id: int) -> Optional[ItemDTO]:
        """Map OAI-PMH record to ItemDTO"""
        dc_data = record.get("metadata", {}).get("oai_dc:dc")
        if not dc_data:
            return None

        return ItemDTO(
            id=None,
            id_repository=repo_id,
            title=self._safe_extract(dc_data, "dc:title"),
            creator=self._safe_extract(dc_data, "dc:creator"),
            subject=self._safe_extract(dc_data, "dc:subject"),
            description=self._safe_extract(dc_data, "dc:description"),
            publisher=self._safe_extract(dc_data, "dc:publisher"),
            contributor=self._safe_extract(dc_data, "dc:contributor"),
            date=self._extract_publication_date(dc_data),
            type=self._safe_extract(dc_data, "dc:type"),
            format=self._safe_extract(dc_data, "dc:format"),
            identifier=self._safe_extract(dc_data, "dc:identifier"),
            source=self._safe_extract(dc_data, "dc:source"),
            language=self._safe_extract(dc_data, "dc:language"),
            relation=self._safe_extract(dc_data, "dc:relation"),
            coverage=self._safe_extract(dc_data, "dc:coverage"),
            rights=self._safe_extract(dc_data, "dc:rights")
        )

    def _dto_to_tuple(self, item_dto: ItemDTO) -> Tuple:
        """Convert ItemDTO to database insertion tuple"""
        return (
            item_dto.id_repository, item_dto.title, item_dto.creator,
            item_dto.subject, item_dto.description, item_dto.publisher,
            item_dto.contributor, item_dto.date, item_dto.type,
            item_dto.format, item_dto.identifier, item_dto.source,
            item_dto.language, item_dto.relation, item_dto.coverage,
            item_dto.rights
        )

    def _batch_insert_items(self, item_data: List[Tuple]) -> bool:
        """Execute batch insert with fallback to individual inserts"""
        sql_template = """
        INSERT INTO items (
            id_repository, title, creator, subject, description, publisher,
            contributor, date, type, format, identifier, source, language,
            relation, coverage, rights
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        try:
            with self.connection.cursor() as cursor:
                execute_batch(cursor, sql_template, item_data)
                self.connection.commit()
                print(f"Successfully batch-inserted {len(item_data)} items.")
                return True

        except Exception as e:
            print(f"Batch insert failed: {e}")
            print("Attempting individual inserts...")
            self.connection.rollback()
            
            # Fallback: individual inserts
            success_count = 0
            fail_count = 0
            
            with self.connection.cursor() as cursor:
                for idx, item_tuple in enumerate(item_data):
                    try:
                        cursor.execute(sql_template, item_tuple)
                        self.connection.commit()
                        success_count += 1
                    except Exception as item_error:
                        print(f"Failed to insert item #{idx}: {item_error}")
                        self.connection.rollback()
                        fail_count += 1
            
            print(f"Individual insert results: {success_count} succeeded, {fail_count} failed")
            return success_count > 0

    def _process_items_for_repository(self, records_list: List[Dict], repo_id: int):
        """Transform records to DTOs and load into database"""
        print(f"Processing {len(records_list)} items for repository ID: {repo_id}...")
        
        existing_identifiers = self._get_existing_identifiers(repo_id)
        item_data_to_insert = []
        skipped_count = 0
        
        for idx, record in enumerate(records_list):
            try:
                item_dto = self._map_record_to_dto(record, repo_id)
                
                if not item_dto:
                    print(f"Skipping record #{idx} with no 'oai_dc:dc' metadata.")
                    skipped_count += 1
                    continue

                # Skip duplicates
                if item_dto.identifier and item_dto.identifier in existing_identifiers:
                    print(f"Skipping record #{idx}: identifier '{item_dto.identifier}' already exists in DB")
                    skipped_count += 1
                    continue

                item_data_to_insert.append(self._dto_to_tuple(item_dto))
                
            except Exception as e:
                print(f"Error processing record #{idx}: {e}")
                skipped_count += 1
                continue

        print(f"Valid items to insert: {len(item_data_to_insert)}")
        print(f"Skipped items: {skipped_count}")
        
        if item_data_to_insert:
            self._batch_insert_items(item_data_to_insert)
        else:
            print("No valid items found to insert.")

    def process_json_directory(self, directory_path: str):
        """Main ETL workflow: scan, extract, transform, load"""
        print(f"--- Starting Full ETL Process: Scanning directory '{directory_path}' ---")

        search_path = os.path.join(directory_path, "*.json")
        json_files = glob.glob(search_path)

        if not json_files:
            print(f"No .json files found in '{directory_path}'.")
            return
        print(f"Found {len(json_files)} JSON file(s) to process.")

        for json_file_path in json_files:
            records_list, repo_url, repository_name = self._extract_data_from_file(json_file_path)

            if records_list is None or not repository_name or not repo_url:
                print(f"Skipping file {json_file_path} due to missing data.")
                continue

            try:
                repo_dto = RepositoryDTO(
                    id=None,
                    name=repository_name,
                    url=repo_url,
                    last_update=date.today(),
                    alive=True
                )
                repo_id = self._find_or_create_repository(repo_dto)
                
                if not repo_id:
                    print(f"Failed to get repository ID for '{repository_name}'. Skipping items.")
                    continue

            except Exception as e:
                print(f"An error occurred creating the repository DTO: {e}. Skipping file.")
                continue

            if records_list:
                self._process_items_for_repository(records_list, repo_id)
            else:
                print("No item records to process for this file.")

            print(f"--- Completed processing for: {repository_name} ---")

    def close_connection(self):
        if self.connection and hasattr(self.connection, 'close'):
            self.connection.close()
            print("\nDatabase connection closed.")


if __name__ == "__main__":
    JSON_DIRECTORY = r"../../data/input/json/"

    # Construct dynamic path from script location
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(script_dir, "../../"))
        JSON_DIRECTORY = os.path.join(project_root, "data", "output", "data")
        print(f"Dynamically constructed JSON path: {JSON_DIRECTORY}")
    except NameError:
        JSON_DIRECTORY = r"../../data/input/json/"
        print(f"Warning: __file__ not defined. Using relative path: {JSON_DIRECTORY}")

    processor = None
    try:
        processor = InsertItemFromRepository()
        processor.process_json_directory(JSON_DIRECTORY)
        print("\n[Test Result] Full ETL process complete.")

    except ConnectionError as e:
        print(f"\nFatal Error: {e}")
        print("Please ensure 'db_connector.py' is in 'src/db/' and your .env credentials are correct.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
    finally:
        if processor:
            processor.close_connection()