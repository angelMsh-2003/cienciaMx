import requests
import xml.etree.ElementTree as ET


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
        params = {'verb': 'ListRecords', 'metadataPrefix': metadataPrefix}
        resumption_token = None
        accumulated_root = None
        accumulated_list_records = None
        namespaces = {'oai': 'http://www.openarchives.org/OAI/2.0/'}

        while True:
            if resumption_token:
                params = {'verb': 'ListRecords', 'resumptionToken': resumption_token}

            response = self.session.get(self.base_url, params=params, timeout=60)
            response.raise_for_status()

            xml_text = response.text
            try:
                parsed_root = ET.fromstring(xml_text)
            except ET.ParseError as exc:
                return {
                    'xml': xml_text,
                    'error': f'XML parse error while fetching records: {exc}',
                    'response_params': params,
                }
            list_records = parsed_root.find('oai:ListRecords', namespaces)

            if list_records is None:
                # No records returned; return the raw XML for debugging
                return {'xml': xml_text}

            # Capture resumption token (if any) before manipulating the tree
            resumption_elem = list_records.find('oai:resumptionToken', namespaces)
            resumption_token = None
            if resumption_elem is not None and resumption_elem.text:
                resumption_token = resumption_elem.text.strip() or None

            # Initialize the accumulator with the first response
            if accumulated_root is None:
                accumulated_root = parsed_root
                accumulated_list_records = list_records
            else:
                for record in list_records.findall('oai:record', namespaces):
                    accumulated_list_records.append(record)

            # Always remove the resumption token from the accumulated tree so it's not included in the final output
            if accumulated_list_records is not None:
                for token in accumulated_list_records.findall('oai:resumptionToken', namespaces):
                    accumulated_list_records.remove(token)

            if not resumption_token:
                break

        final_xml = ET.tostring(accumulated_root, encoding='unicode')
        return {'xml': final_xml}
    
    def get_collections(self):
        pass

    
    