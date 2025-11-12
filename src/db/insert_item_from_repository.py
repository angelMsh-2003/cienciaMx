import json
import os
import sys
import glob
from datetime import date
from urllib.parse import urlparse
from typing import Optional, List, Dict, Any, Tuple
from src.db.db_connector import get_db_connection
from src.db.repository_DTO import RepositoryDTO

# We'll need this for batch inserting
try:
    from psycopg2.extras import execute_batch
except ImportError:
    print("Warning: psycopg2.extras not found. Batch insert may not be optimized.")


    # Fallback function if psycopg2 is not used or available
    def execute_batch(cursor, sql, argslist):
        for args in argslist:
            cursor.execute(sql, args)

# --- Imports ---
try:
    from src.db.item_DTO import ItemDTO
    from src.db.repository_DTO import RepositoryDTO
    from src.db.db_connector import get_db_connection
except ImportError:
    print("Warning: Could not import DTOs or db_connector from 'src.db'.")


    # ... (placeholder classes) ...
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

            def fetchone(self): return (1,)  # Return a mock ID

        return MockConnection()


class InsertItemFromRepository:
    """
    Orchestrates the ETL process for DSpace JSON exports.
    1. Scans directory for JSON files.
    2. Creates/updates a 'repository' entry for each file.
    3. Maps and batch-inserts all 'item' records from each file.
    """

    def __init__(self):
        # Initializate Database Connection
        self.connection = get_db_connection()
        if not self.connection:
            raise ConnectionError("Failed to get database connection.")
        print("Database connection established.")

    # --- Step 1 Helper: Extract data from JSON file ---
    def _extract_data_from_file(self, json_file_path: str) -> Tuple[Optional[List], Optional[str], Optional[str]]:
        """
        Private method to read a single JSON file, extract the record list,
        and get the repository name and url.
        """
        print(f"\n--- Reading file: {os.path.basename(json_file_path)} ---")
        # ... (This method is correct from last time) ...
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

    # --- Step 2 Helper: Find or create repository ---
    def _find_or_create_repository(self, repo_dto: RepositoryDTO) -> Optional[int]:
        """
        Finds a repository by name. If it exists, updates it.
        If not, inserts it. Returns the repository ID.
        """
        print(f"Processing repository: {repo_dto.name}...")
        try:
            with self.connection.cursor() as cursor:
                # 1. Check if repository exists
                # --- FIXED: 'repository' changed to 'repositories' ---
                cursor.execute(
                    "SELECT id FROM repositories WHERE name = %s;",
                    (repo_dto.name,)
                )
                result = cursor.fetchone()

                repo_id: Optional[int] = None

                if result:
                    # 2. EXISTS: Update it
                    repo_id = result[0]
                    print(f"Repository '{repo_dto.name}' found (ID: {repo_id}). Updating.")
                    # --- FIXED: 'repository' changed to 'repositories' ---
                    cursor.execute(
                        """
                        UPDATE repositories
                        SET last_update = %s, alive = %s, url = %s
                        WHERE id = %s;
                        """,
                        (repo_dto.last_update, repo_dto.alive, repo_dto.url, repo_id)
                    )
                else:
                    # 3. DOES NOT EXIST: Insert it
                    print(f"Repository '{repo_dto.name}' not found. Creating new entry.")
                    # --- FIXED: 'repository' changed to 'repositories' ---
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

    # --- Step 3 Helpers: Item Processing ---

    def _safe_extract(self, dc_data: Dict[str, Any], key: str) -> Optional[str]:
        """
        Safely extracts and formats metadata values (handles lists/objects).
        """
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
        """
        Finds and parses the main publication date, ignoring embargo dates.
        """
        date_list = dc_data.get("dc:date")
        if not isinstance(date_list, list):
            return None

        for date_str in date_list:
            if date_str and not date_str.startswith("info:eu-repo"):
                try:
                    # Handle full YYYY-MM-DD
                    return date.fromisoformat(date_str)
                except (ValueError, TypeError):
                    # Handle just YYYY
                    if len(date_str) == 4 and date_str.isdigit():
                        return date.fromisoformat(f"{date_str}-01-01")
                    continue
        return None

    def _process_items_for_repository(self, records_list: List[Dict], repo_id: int):
        """
        Maps all records to ItemDTOs and performs a batch insert.
        """
        print(f"Processing {len(records_list)} items for repository ID: {repo_id}...")
        item_data_to_insert = []

        # 1. Map all records to tuples for insertion
        for record in records_list:
            dc_data = record.get("metadata", {}).get("oai_dc:dc")
            if not dc_data:
                print("Skipping record with no 'oai_dc:dc' metadata.")
                continue

            # Map to ItemDTO fields
            item_dto = ItemDTO(
                id=None,  # DB generates this
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

            # Convert DTO to a tuple in the correct DB order
            item_tuple = (
                item_dto.id_repository, item_dto.title, item_dto.creator,
                item_dto.subject, item_dto.description, item_dto.publisher,
                item_dto.contributor, item_dto.date, item_dto.type,
                item_dto.format, item_dto.identifier, item_dto.source,
                item_dto.language, item_dto.relation, item_dto.coverage,
                item_dto.rights
            )
            item_data_to_insert.append(item_tuple)

        # 2. Perform the batch insert
        if not item_data_to_insert:
            print("No valid items found to insert.")
            return

        try:
            with self.connection.cursor() as cursor:
                # This SQL template must match the tuple order above
                sql_template = """
                INSERT INTO items (
                    id_repository, title, creator, subject, description, publisher,
                    contributor, date, type, format, identifier, source, language,
                    relation, coverage, rights
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                # Use execute_batch for high performance
                execute_batch(cursor, sql_template, item_data_to_insert)

                self.connection.commit()
                print(f"Successfully batch-inserted {len(item_data_to_insert)} items.")

        except Exception as e:
            print(f"Database error during batch item insert: {e}", file=sys.stderr)
            self.connection.rollback()

    # --- Main Control Flow ---

    def process_json_directory(self, directory_path: str):
        """
        Runs the full ETL process: Scan dir, process repos, process items.
        """
        print(f"--- Starting Full ETL Process: Scanning directory '{directory_path}' ---")

        search_path = os.path.join(directory_path, "*.json")
        json_files = glob.glob(search_path)

        if not json_files:
            print(f"No .json files found in '{directory_path}'.")
            return
        print(f"Found {len(json_files)} JSON file(s) to process.")

        for json_file_path in json_files:
            # --- STEP 1: EXTRACT DATA ---
            records_list, repo_url, repository_name = self._extract_data_from_file(json_file_path)

            if records_list is None or not repository_name or not repo_url:
                print(f"Skipping file {json_file_path} due to missing data.")
                continue

            # --- STEP 2: CREATE/UPDATE REPOSITORY ---
            repo_id: Optional[int] = None
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

            # --- STEP 3: PROCESS & BATCH-INSERT ITEMS ---
            if records_list:
                self._process_items_for_repository(records_list, repo_id)
            else:
                print("No item records to process for this file.")

            print(f"--- Completed processing for: {repository_name} ---")

    def close_connection(self):
        """
        Closes the database connection if it exists.
        """
        if self.connection and hasattr(self.connection, 'close'):
            self.connection.close()
            print("\nDatabase connection closed.")


# --- Main execution block ---
if __name__ == "__main__":
    """
    This block allows you to run this file directly to test
    the full ETL process.
    """
    JSON_DIRECTORY = r"../../data/input/json/"

    # --- Robust Path (from previous discussion) ---
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(script_dir, "../../"))  # Up two levels
        JSON_DIRECTORY = os.path.join(project_root, "data", "input", "json")
        print(f"Dynamically constructed JSON path: {JSON_DIRECTORY}")
    except NameError:
        JSON_DIRECTORY = r"../../data/input/json/"
        print(f"Warning: __file__ not defined. Using relative path: {JSON_DIRECTORY}")
    # --- End Robust Path ---

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