import os
import re
import json
import pandas as pd  # Used to read the CSV

# Import the client from the other file
from dspace_client import DSpaceClient
    
def safe_filename(name: str) -> str:
    """
    Creates a filesystem-safe filename from a string.
    """
    name = name.strip()
    name = re.sub(r"[^A-Za-z0-9 \-_]", "_", name)
    name = re.sub(r"[\s_]+", "_", name)
    return name

def main():
    """
    Main function to read a CSV, filter repositories,
    and harvest records from each one.
    """
    
    # --- Configuration ---
    CSV_FILE_PATH = r'data/input/csv/WORKING REPORT (repositorios_proof_of_life) - repositorios_proof_of_life .csv'
    OUTPUT_DIRECTORY = r'data/output/data2' 
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

    # Read the CSV file
    try:
        df = pd.read_csv(CSV_FILE_PATH)
    except FileNotFoundError:
        print(f"FATAL ERROR: CSV file not found at: {CSV_FILE_PATH}")
        return
    except Exception as e:
        print(f"FATAL ERROR: Could not read CSV file: {e}")
        return

    # Clean whitespace (like 'alive_item ') from column names
    df.columns = df.columns.str.strip()

    # --- FILTER LOGIC ---
    # Filter for 'DSpace' repositories, with an OAI URL, and 'alive_item == True'
    print("Filtering for 'DSpace' repositories with 'alive_item == True' (any version)...")
    df_filtered = df[(df['software'] == 'DSpace') & 
                     (df['alive_item'] == True) &
                     (df['oai_url'].notna())] 

    print(f"Found {len(df_filtered)} DSpace repositories with an OAI URL and 'alive_item == True' to process.")

    # Iterate over each filtered repository
    for index, row in df_filtered.iterrows():
        repo_name = row.iloc[0]  # The first column is the name
        oai_url = row['oai_url']

        print(f"\nProcessing {index}: {repo_name} ({oai_url})")

        base_url = oai_url.replace('?verb=Identify', '')

        try:
            client = DSpaceClient(base_url)
            output = client.get_records()

        except ImportError as e:
            print(f"  !! SKIPPING: {e}")
            continue 
        except Exception as e:
            print(f"  !! FAILED harvest for {repo_name}: {e}")
            output = {'error_general': str(e)}

        # 3. Save the results 
        
        if 'error_general' in output or 'error' in output:
            print("  ...An error was found during harvest. Check data for details.")
        else:
            record_count = 0
            try:
                record_count = len(output.get('OAI-PMH', {}).get('ListRecords', {}).get('record', []))
            except TypeError:
                pass 
            print(f"  ...Harvest successful. Total records parsed: {record_count}")

        # Define JSON filename
        filename_base = safe_filename(str(repo_name)) or f'repo_{index}'
        json_out_path = os.path.join(OUTPUT_DIRECTORY, f"{filename_base}.json")

        # Save JSON file
        try:
            print(f"  ...Saving JSON to: {json_out_path}")
            with open(json_out_path, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f"  !! ERROR: Could not save JSON file: {e}")


    print("\n\nAll repositories processed.")


# Dependency check
if __name__ == "__main__":
    if pd is not None:
        main()
    else:
        print("Script cannot run. Check if 'pandas' is installed.")