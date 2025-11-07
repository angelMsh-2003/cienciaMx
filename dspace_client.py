import requests
import json


class DSpaceClient:
    """
    A client for interacting with DSpace OAI-PMH API.
    """

    def __init__(self, base_url):
        """
        Initialize the DSpace client.

        :param base_url: The base URL of the OAI endpoint (e.g., 'https://demo.dspace.org/oai/request')
        """
        self.base_url = base_url.rstrip('?')
        self.session = requests.Session()

    def get_records(self, metadataPrefix='oai_dc'):
        """
        Retrieve records using ListRecords verb.

        :param metadataPrefix: Metadata prefix (default: 'oai_dc')
        :return: Dict with XML content
        :raises requests.HTTPError: If the request fails
        """
        url = f"{self.base_url}?verb=ListRecords&metadataPrefix={metadataPrefix}"
        response = self.session.get(url)
        response.raise_for_status()
        return {'xml': response.text}
    
    def get_collections():
        pass

    
    