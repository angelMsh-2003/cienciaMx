import requests
import json

try:
    # xmltodict is a crucial dependency for parsing
    import xmltodict
except ImportError:
    xmltodict = None

class DSpaceClient:
    """
    A client for interacting with the DSpace OAI-PMH API.
    Handles pagination automatically via resumptionToken.
    """

    def __init__(self, base_url):
        """
        Initialize the client.
        :param base_url: The base URL of the OAI endpoint (e.g., 'https://demo.dspace.org/oai/request')
        """
        if xmltodict is None:
            # Fail fast if the required library isn't installed
            raise ImportError("The 'xmltodict' library is required. Please install it: pip install xmltodict")
            
        self.base_url = base_url.rstrip('?')
        self.session = requests.Session()

    def get_records(self, metadataPrefix='oai_dc'):
        """
        Retrieve ALL records, handling OAI-PMH pagination.
        This method will loop until all records are fetched.

        :param metadataPrefix: Metadata prefix (default: 'oai_dc')
        :return: A single Dict containing all parsed records.
        :raises requests.HTTPError: If the request fails
        :raises Exception: If XML parsing fails
        """
        
        # 1. Parameters for the first page
        params = {'verb': 'ListRecords', 'metadataPrefix': metadataPrefix}
        
        # 2. Make the first request
        print(f"  ...Starting request to {self.base_url}")
        try:
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status() # Raise an exception for bad status codes
            
            # 3. Parse the first page
            full_data = xmltodict.parse(response.text)
            
            list_records_node = full_data.get('OAI-PMH', {}).get('ListRecords', {})
            all_records_list = list_records_node.get('record', [])

            if all_records_list and not isinstance(all_records_list, list):
                all_records_list = [all_records_list]

        except Exception as e:
            error_text = response.text if 'response' in locals() else 'No response'
            return {'error': f'XML parsing error on the first page: {e}', 'xml_text': error_text[:1000]}

        # 4. Start the pagination loop
        while True:
            # 5. Find the resumptionToken from the *last* page we loaded
            token_node = list_records_node.get('resumptionToken')
            token = None
            if token_node:
                token = token_node.get('#text') if isinstance(token_node, dict) else token_node

            # 6. If there's no token, or it's empty, we are done.
            if not token:
                print("  ...No resumptionToken found. Pagination complete.")
                break
            
            # 7. Use the token to request the next page
            print(f"  ...Fetching next page with token: {token[:30]}...")
            params = {'verb': 'ListRecords', 'resumptionToken': token}
            
            try:
                response = self.session.get(self.base_url, params=params)
                response.raise_for_status()

                # 8. Parse the new page
                page_data = xmltodict.parse(response.text)
                list_records_node = page_data.get('OAI-PMH', {}).get('ListRecords', {}) 
                page_records_list = list_records_node.get('record', [])

                if page_records_list and not isinstance(page_records_list, list):
                    page_records_list = [page_records_list]
                
                # 9. Add the records from this page to our total list
                if page_records_list:
                    all_records_list.extend(page_records_list)
            
            except Exception as e:
                print(f"  !! ERROR: Failed to parse page with token {token}: {e}")
                print("  ...Stopping pagination and saving results obtained so far.")
                break # Exit the loop

        # 10. We're done. Overwrite the 'record' list.
        if 'OAI-PMH' in full_data and 'ListRecords' in full_data['OAI-PMH']:
            full_data['OAI-PMH']['ListRecords']['record'] = all_records_list
        
            if 'resumptionToken' in full_data['OAI-PMH']['ListRecords']:
                del full_data['OAI-PMH']['ListRecords']['resumptionToken']
        else:
            if 'error' not in full_data:
                 return {'error': 'Invalid OAI-PMH response, ListRecords not found', 'data': full_data}

        return full_data