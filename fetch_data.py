import os
import re
import json
import pandas as pd
from dspace_client import DSpaceClient

try:
    import xmltodict
except Exception:
    xmltodict = None


# Utility to make a filesystem-safe filename from repository name
def safe_filename(name: str) -> str:
    # Keep letters, numbers, spaces, hyphens and underscores; replace others with _
    name = name.strip()
    name = re.sub(r"[^A-Za-z0-9 \-_]", "_", name)
    # Collapse multiple underscores/spaces
    name = re.sub(r"[\s_]+", "_", name)
    return name


# Ensure data directory exists
os.makedirs('data', exist_ok=True)

# Read the CSV file
df = pd.read_csv(r'paths/WORKING REPORT (repositorios_proof_of_life) - repositorios_proof_of_life.csv')

# Filter for DSpace version 5 repositories
df_dspace5 = df[(df['software'] == 'DSpace') & (df['version'].str.startswith('6')) & (df['oai_url'].notna())]

for index, row in df_dspace5.iterrows():
    repo_name = row.iloc[0]  # First column is the repository name
    oai_url = row['oai_url']

    print(f"Processing {repo_name}")

    # Base URL is OAI URL without the verb
    base_url = oai_url.replace('?verb=Identify', '')

    client = DSpaceClient(base_url)

    try:
        records = client.get_records()
        data = records
    except Exception as e:
        data = {'error': str(e)}

    # If the client returned XML (dict with key 'xml'), try converting to JSON using xmltodict
    output = None
    if isinstance(data, dict) and 'xml' in data and data['xml'] is not None:
        if xmltodict is None:
            # xmltodict not installed — keep original XML in output with a hint
            output = {'error': 'xmltodict not installed', 'xml': data['xml']}
        else:
            try:
                parsed = xmltodict.parse(data['xml'])
                output = parsed
            except Exception as e:
                output = {'error': f'xml parsing error: {e}'}
    else:
        # Not XML — write whatever the client returned
        output = data

    # Safe filename and write JSON (one file per source)
    filename = safe_filename(str(repo_name)) or f'repo_{index}'
    out_path = os.path.join('data', f"{filename}.json")
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=4, ensure_ascii=False)