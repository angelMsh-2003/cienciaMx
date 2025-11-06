import requests
from urllib.parse import urljoin
import csv

# Lista de rutas OAI-PMH más comunes
ENDPOINT_PATHS = [
    "oai/request",
    "xmlui/oai/request",
    "jspui/oai/request",
    "oai/conacyt",
    "oai/public",
    "oai/openaire",
    "oai/literatura",
    "cgi/oai2",
    "oai2",
    "oai2d",
    "resource/oai-pmh",
    "provider",
    "server/oai/request",
    "catalog/oai",
    "conacyt/oai/oai2.php",
    "oai-pmh-repository/request",
    "vufind/OAI/conacyt",
]

# Encabezados simulando un navegador
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/121.0.0.0 Safari/537.36",
    "Accept": "application/xml, text/xml;q=0.9, */*;q=0.8",
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}


def check_endpoints(base_url):
    print(f"\n🔍 Checking OAI-PMH endpoints for: {base_url}\n")
    results = []
    found = []
    timeout_list = []
    unauthorized = []
    not_found = []

    for path in ENDPOINT_PATHS:
        endpoint_url = urljoin(base_url, path)
        test_url = endpoint_url.rstrip("/") + "?verb=Identify"
        try:
            response = requests.get(
                test_url,
                headers=HEADERS,
                timeout=(10, 60),   # tiempos mayores para servidores lentos
                allow_redirects=True,
                verify=False
            )

            status = response.status_code

            # Revisión de estado
            if status == 200 and "<Identify" in response.text:
                print(f"✅ FOUND: {test_url}")
                found.append(endpoint_url)
                results.append([endpoint_url, "FOUND", status])
            elif status in (401, 403):
                print(f"🔒 Unauthorized: {test_url} (status {status})")
                unauthorized.append(endpoint_url)
                results.append([endpoint_url, "UNAUTHORIZED", status])
            elif status == 468:
                print(f"⚠️ Filtered/Blocked (468): {test_url}")
                results.append([endpoint_url, "BLOCKED", status])
            else:
                print(f"❌ Unavailable: {test_url} (status {status})")
                not_found.append(endpoint_url)
                results.append([endpoint_url, "UNAVAILABLE", status])

        except requests.exceptions.Timeout:
            print(f"⏳ Timeout (slow server): {test_url}")
            timeout_list.append(endpoint_url)
            results.append([endpoint_url, "TIMEOUT", ""])
        except Exception as e:
            print(f"⚠️ Error with {test_url}: {e}")
            not_found.append(endpoint_url)
            results.append([endpoint_url, "ERROR", str(e)])

    # --- Summary ---
    print("\n--- Summary ---")
    print(f"✅ Found endpoints: {len(found)}")
    for f in found:
        print(f"  • {f}")
    print(f"\n🔒 Unauthorized endpoints: {len(unauthorized)}")
    for u in unauthorized:
        print(f"  • {u}")
    print(f"\n⏳ Timeout endpoints: {len(timeout_list)}")
    for t in timeout_list:
        print(f"  • {t}")
    print(f"\n❌ Unavailable endpoints: {len(not_found)}")

# Ejemplo de uso
if __name__ == "__main__":
    repo_url = input("Enter repository base URL (e.g., https://ri.uaemex.mx/): ").strip()
    if not repo_url.endswith("/"):
        repo_url += "/"
    check_endpoints(repo_url)
