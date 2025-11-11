import requests
import xml.etree.ElementTree as ET
import json
import urllib3
import pandas as pd
from urllib.parse import urlparse
import os 

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_sets(base_url):
    """Gets all sets from the repository and returns only those of type 'col' and 'com'."""
    r = requests.get(f"{base_url}?verb=ListSets", verify=False)
    root = ET.fromstring(r.text)
    sets = []
    for s in root.findall(".//{http://www.openarchives.org/OAI/2.0/}set"):
        spec = s.find("{http://www.openarchives.org/OAI/2.0/}setSpec").text
        name = s.find("{http://www.openarchives.org/OAI/2.0/}setName").text
        sets.append((spec, name))
    return sets

def count_items_in_set(base_url, set_spec):
    count = 0
    token = None
    while True:
        url = f"{base_url}?verb=ListRecords&set={set_spec}&metadataPrefix=oai_dc"
        if token:
            url = f"{base_url}?verb=ListRecords&resumptionToken={token}"
        r = requests.get(url, verify=False)
        root = ET.fromstring(r.text)
        count += len(root.findall(".//{http://www.openarchives.org/OAI/2.0/}record"))
        token_el = root.find(".//{http://www.openarchives.org/OAI/2.0/}resumptionToken")
        if token_el is not None and token_el.text:
            token = token_el.text
        else:
            break
    return count

def process_repository(base_url):
    print(f"\n📘 Processing repository: {base_url}")

    try:
        all_sets = get_sets(base_url)
    except Exception as e:
        print(f"❌ Error getting sets from {base_url}: {e}")
        return None

    communities = {spec: name for spec, name in all_sets if spec.startswith("com_")}
    collections = [(spec, name) for spec, name in all_sets if spec.startswith("col_")]

    report_data = {}

    for spec, name in collections:
        try:
            parts = spec.split("_")
            repo_id = int(parts[1]) if len(parts) > 2 else None
            col_id = int(parts[2]) if len(parts) > 2 else None
            community_key = f"com_{repo_id}"
            community_name = communities.get(community_key, "Unknown Community")

            total = count_items_in_set(base_url, spec)

            if community_name not in report_data:
                report_data[community_name] = []

            report_data[community_name].append({
                "name": name,
                "type": spec,
                "repository_id": repo_id,
                "collection_id": col_id,
                "total_items": total
            })
        except Exception as e:
            print(f"⚠️ Error processing {name} ({spec}): {e}")

    return report_data


csv_file = "dataversion5.0.csv"  # Make sure your file is named this way
try:
    df = pd.read_csv(csv_file)
except FileNotFoundError:
    print(f"❌ ERROR: File not found '{csv_file}'. Make sure it exists.")
    exit()  # We exit the script if there is no input file
except Exception as e:
    print(f"❌ ERROR: Could not read the CSV file: {e}")
    exit()


# === Process each URL ===
for url in df["oai_url"]:
    base = url.strip()
    if not base:
        continue

    parsed = urlparse(base)
    repo_name = parsed.netloc.replace("www.", "").split("/")[0]

    result = process_repository(base)
    if result:
        
        # --- START OF MODIFICATION ---

        output_dir = "json/itemsrepositories"

        os.makedirs(output_dir, exist_ok=True) 

        output_file = os.path.join(output_dir, f"report_{repo_name}.json")
        
        # --- END OF MODIFICATION ---

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=4, ensure_ascii=False)
        print(f"✅ Report generated: {output_file}")
    else:
        print(f"❌ Could not generate report for {base}")

print("\n Process completed.")







