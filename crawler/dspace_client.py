import os
import re
import json
import time
import requests
from datetime import datetime
from urllib.parse import urlparse
import xml.etree.ElementTree as ET
from xml.dom import minidom

try:
    import xmltodict
except ImportError:
    xmltodict = None

from .endpoint_manager import EndpointManager


class DSpaceClient:
    """
    Enhanced client for interacting with DSpace OAI-PMH API.
    Uses EndpointManager to manage endpoints and handles pagination.
    """

    def __init__(self, endpoint_identifier=None, base_url=None):
        """
        Initialize the DSpace client.
        
        :param endpoint_identifier: ID or URL of endpoint from EndpointManager
        :param base_url: Direct base URL of the OAI endpoint (fallback)
        """
        self.endpoint_manager = EndpointManager()
        self.endpoint = None
        self.session = requests.Session()
        
        # Set up headers to avoid being blocked
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        if endpoint_identifier:
            self.endpoint = self.endpoint_manager.get_endpoint(endpoint_identifier)
            if self.endpoint:
                self.base_url = self.endpoint['url']
            else:
                raise ValueError(f"Endpoint {endpoint_identifier} not found")
        elif base_url:
            self.base_url = base_url.rstrip('?')
        else:
            raise ValueError("Either endpoint_identifier or base_url must be provided")

    def safe_filename(self, name: str) -> str:
        """Create a filesystem-safe filename from repository name."""
        name = name.strip()
        name = re.sub(r"[^A-Za-z0-9 \-_]", "_", name)
        name = re.sub(r"[\s_]+", "_", name)
        return name

    def generate_machine_name(self, name: str) -> str:
        """
        Generate a machine-friendly identifier from repository name.
        Creates a lowercase, underscore-separated version suitable for programmatic use.
        """
        # Clean and normalize the name
        clean_name = name.strip().lower()
        # Replace any non-alphanumeric characters with underscores
        clean_name = re.sub(r"[^a-z0-9]+", "_", clean_name)
        # Remove multiple consecutive underscores and trim
        clean_name = re.sub(r"_+", "_", clean_name).strip("_")
        return clean_name

    def ensure_machine_name(self):
        """Ensure the endpoint has a machine_name field, add it if missing."""
        if not self.endpoint:
            return None
            
        # Check if machine_name already exists
        machine_name = self.endpoint.get('machine_name')
        if machine_name:
            return machine_name
        
        # Generate machine name from the repository name
        repo_name = self.endpoint.get('name', 'unknown_repository')
        machine_name = self.generate_machine_name(repo_name)
        
        # Add uniqueness if needed (for repositories with same name)
        existing_machine_names = [
            ep.get('machine_name', '') for ep in self.endpoint_manager.list_endpoints()
            if ep.get('machine_name') and ep.get('id') != self.endpoint.get('id')
        ]
        
        # If machine name already exists, add the ID to make it unique
        if machine_name in existing_machine_names:
            repo_id = self.endpoint.get('id', 'unknown')
            machine_name = f"{machine_name}_{repo_id}"
        
        # Use EndpointManager to set the machine_name field
        success = self.endpoint_manager.set_nested_field(
            self.endpoint['id'],
            'machine_name',
            machine_name,
            allowed_fields=['machine_name']
        )
        
        if success:
            # Update local endpoint reference
            self.endpoint['machine_name'] = machine_name
            return machine_name
        
        return None

    def get_repository_folder_name(self) -> str:
        """Generate human-readable folder name from endpoint data."""
        if not self.endpoint:
            return "unknown_repository"
        
        # Use the name from endpoints.json
        repo_name = self.endpoint.get('name', 'Unknown Repository')
        
        # Clean and format the name
        safe_name = self.safe_filename(repo_name)
        
        # Add ID for uniqueness if needed
        repo_id = self.endpoint.get('id', 'unknown')
        
        return f"{safe_name}_ID_{repo_id}"

    def parse_resumption_token(self, xml_content: str) -> tuple:
        """
        Parse resumption token from OAI-PMH response to handle pagination.
        
        :param xml_content: XML response content
        :return: (has_more, next_token, error_message)
        """
        try:
            root = ET.fromstring(xml_content)
            
            # Check for errors in the response
            error_elem = root.find('.//error')
            if error_elem is not None:
                return False, None, error_elem.text
            
            # Look for resumptionToken
            resumption_token = root.find('.//resumptionToken')
            if resumption_token is not None:
                token_text = resumption_token.text
                if token_text:
                    return True, token_text, None
            
            return False, None, None
            
        except ET.ParseError as e:
            return False, None, f"XML parse error: {str(e)}"

    def get_records_page(self, metadataPrefix='oai_dc', resumptionToken=None):
        """
        Retrieve a single page of records using ListRecords verb.
        
        :param metadataPrefix: Metadata prefix (default: 'oai_dc')
        :param resumptionToken: Token for pagination (None for first request)
        :return: Dict with XML content and pagination info
        :raises requests.HTTPError: If the request fails
        """
        if resumptionToken:
            url = f"{self.base_url}?verb=ListRecords&resumptionToken={resumptionToken}"
        else:
            url = f"{self.base_url}?verb=ListRecords&metadataPrefix={metadataPrefix}"
        
        response = self.session.get(url)
        response.raise_for_status()
        
        # Parse resumption token for pagination
        has_more, next_token, error_msg = self.parse_resumption_token(response.text)
        
        return {
            'xml': response.text,
            'has_more': has_more,
            'next_token': next_token,
            'error': error_msg
        }

    def get_all_records_paginated(self, metadataPrefix='oai_dc', max_pages=None, delay=1.0):
        """
        Retrieve all records from endpoint with proper pagination handling.
        
        :param metadataPrefix: Metadata prefix (default: 'oai_dc')
        :param max_pages: Maximum number of pages to retrieve (None for unlimited)
        :param delay: Delay between requests in seconds
        :return: List of all XML responses
        """
        all_xml_responses = []
        current_token = None
        page_count = 0
        
        while True:
            try:
                print(f"Fetching page {page_count + 1}..." if page_count > 0 else "Fetching first page...")
                
                result = self.get_records_page(metadataPrefix, current_token)
                
                if result['error']:
                    print(f"OAI-PMH Error: {result['error']}")
                    break
                
                all_xml_responses.append(result['xml'])
                page_count += 1
                
                # Check if we should continue
                if not result['has_more'] or not result['next_token']:
                    print("No more pages available.")
                    break
                
                if max_pages and page_count >= max_pages:
                    print(f"Reached maximum pages limit: {max_pages}")
                    break
                
                current_token = result['next_token']
                
                # Add delay to be respectful to the server
                if delay > 0:
                    time.sleep(delay)
                    
            except requests.RequestException as e:
                print(f"Request error: {e}")
                break
            except Exception as e:
                print(f"Unexpected error: {e}")
                break
        
        print(f"Total pages retrieved: {page_count}")
        return all_xml_responses

    def xml_to_dict(self, xml_content: str) -> dict:
        """Convert XML content to dictionary using xmltodict if available."""
        if xmltodict is None:
            return {'error': 'xmltodict not available', 'raw_xml': xml_content}
        
        try:
            return xmltodict.parse(xml_content)
        except Exception as e:
            return {'error': f'XML parsing error: {str(e)}', 'raw_xml': xml_content}

    def get_records_xml(self, output_dir='data', metadataPrefix='oai_dc', max_pages=None, delay=1.0, shared_timestamp=None):
        """
        Main method to retrieve and save all records as XML files.
        
        :param output_dir: Output directory (default: 'data')
        :param metadataPrefix: Metadata prefix (default: 'oai_dc')
        :param max_pages: Maximum pages to retrieve
        :param delay: Delay between requests
        :param shared_timestamp: Shared timestamp for batch operations (optional)
        :return: Dictionary with results summary
        """
        if not self.endpoint:
            raise ValueError("This method requires an endpoint to be set via endpoint_identifier")
        
        # Ensure machine_name exists and is up to date
        machine_name = self.ensure_machine_name()
        
        # Use shared timestamp if provided, otherwise create new one
        timestamp = shared_timestamp if shared_timestamp else datetime.now().strftime("%Y%m%d_%H%M%S")
        repo_folder = self.get_repository_folder_name()
        
        # Create repository-specific subdirectory within the shared output directory
        repo_output_dir = os.path.join(output_dir, repo_folder)
        
        os.makedirs(repo_output_dir, exist_ok=True)
        
        # Retrieve all pages
        print(f"Starting XML collection for: {self.endpoint['name']}")
        print(f"Output directory: {repo_output_dir}")
        
        all_xml_responses = self.get_all_records_paginated(
            metadataPrefix=metadataPrefix,
            max_pages=max_pages,
            delay=delay
        )
        
        if not all_xml_responses:
            print("No data retrieved.")
            return {
                'success': False,
                'message': 'No data retrieved',
                'endpoint': self.endpoint['name'],
                'files_created': 0,
                'output_directory': repo_output_dir
            }
        
        # Save each page as a separate XML file
        files_created = []
        for i, xml_content in enumerate(all_xml_responses, 1):
            filename = f"page_{i:03d}.xml"
            filepath = os.path.join(repo_output_dir, filename)
            
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(xml_content)
                files_created.append(filepath)
                print(f"Saved: {filename}")
            except Exception as e:
                print(f"Error saving {filename}: {e}")
        
        # Update endpoint metadata
        self.endpoint_manager.update_last_crawled(self.endpoint['url'])
        
        # Save metadata
        metadata = {
            'endpoint_info': {
                'id': self.endpoint['id'],
                'name': self.endpoint['name'],
                'machine_name': machine_name,
                'url': self.endpoint['url'],
                'dspace_version': self.endpoint.get('dspace_version'),
                'number_of_items': self.endpoint.get('number_of_items')
            },
            'collection_info': {
                'timestamp': timestamp,
                'total_pages': len(all_xml_responses),
                'metadata_prefix': metadataPrefix,
                'total_files_created': len(files_created)
            },
            'files': files_created
        }
        
        metadata_file = os.path.join(repo_output_dir, 'metadata.json')
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        print(f"Collection complete. Created {len(files_created)} XML files.")
        
        return {
            'success': True,
            'endpoint': self.endpoint['name'],
            'total_pages': len(all_xml_responses),
            'files_created': len(files_created),
            'output_directory': repo_output_dir,
            'metadata_file': metadata_file,
            'files': files_created
        }

    def get_records_json(self, output_dir='data', metadataPrefix='oai_dc', max_pages=None, delay=1.0, shared_timestamp=None):
        """
        Main method to retrieve and save all records as JSON files.
        Transforms XML to JSON using xmltodict.
        
        :param output_dir: Output directory (default: 'data')
        :param metadataPrefix: Metadata prefix (default: 'oai_dc')
        :param max_pages: Maximum pages to retrieve
        :param delay: Delay between requests
        :param shared_timestamp: Shared timestamp for batch operations (optional)
        :return: Dictionary with results summary
        """
        if not self.endpoint:
            raise ValueError("This method requires an endpoint to be set via endpoint_identifier")
        
        # Ensure machine_name exists and is up to date
        machine_name = self.ensure_machine_name()
        
        # Use shared timestamp if provided, otherwise create new one
        timestamp = shared_timestamp if shared_timestamp else datetime.now().strftime("%Y%m%d_%H%M%S")
        repo_folder = self.get_repository_folder_name()
        
        # Create repository-specific subdirectory within the shared output directory
        repo_output_dir = os.path.join(output_dir, repo_folder)
        
        os.makedirs(repo_output_dir, exist_ok=True)
        
        # Retrieve all pages first
        print(f"Starting JSON collection for: {self.endpoint['name']}")
        print(f"Output directory: {repo_output_dir}")
        
        all_xml_responses = self.get_all_records_paginated(
            metadataPrefix=metadataPrefix,
            max_pages=max_pages,
            delay=delay
        )
        
        if not all_xml_responses:
            print("No data retrieved.")
            return {
                'success': False,
                'message': 'No data retrieved',
                'endpoint': self.endpoint['name'],
                'files_created': 0,
                'output_directory': repo_output_dir
            }
        
        # Convert and save each page as JSON
        files_created = []
        for i, xml_content in enumerate(all_xml_responses, 1):
            filename = f"page_{i:03d}.json"
            filepath = os.path.join(repo_output_dir, filename)
            
            try:
                # Convert XML to JSON
                json_data = self.xml_to_dict(xml_content)
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, indent=2, ensure_ascii=False)
                
                files_created.append(filepath)
                print(f"Saved: {filename}")
            except Exception as e:
                print(f"Error saving {filename}: {e}")
        
        # Update endpoint metadata
        self.endpoint_manager.update_last_crawled(self.endpoint['url'])
        
        # Save metadata
        metadata = {
            'endpoint_info': {
                'id': self.endpoint['id'],
                'name': self.endpoint['name'],
                'machine_name': machine_name,
                'url': self.endpoint['url'],
                'dspace_version': self.endpoint.get('dspace_version'),
                'number_of_items': self.endpoint.get('number_of_items')
            },
            'collection_info': {
                'timestamp': timestamp,
                'total_pages': len(all_xml_responses),
                'metadata_prefix': metadataPrefix,
                'total_files_created': len(files_created),
                'xmltodict_available': xmltodict is not None
            },
            'files': files_created
        }
        
        metadata_file = os.path.join(repo_output_dir, 'metadata.json')
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        print(f"Collection complete. Created {len(files_created)} JSON files.")
        
        return {
            'success': True,
            'endpoint': self.endpoint['name'],
            'total_pages': len(all_xml_responses),
            'files_created': len(files_created),
            'output_directory': repo_output_dir,
            'metadata_file': metadata_file,
            'files': files_created
        }

    def get_collections(self):
        """
        Get collections from the repository using ListCollections verb.
        
        :return: Dict with collections data
        """
        url = f"{self.base_url}?verb=ListCollections"
        response = self.session.get(url)
        response.raise_for_status()
        
        return {'xml': response.text}

    def identify_repository(self):
        """
        Get repository identification information using Identify verb.
        
        :return: Dict with identification data
        """
        url = f"{self.base_url}?verb=Identify"
        response = self.session.get(url)
        response.raise_for_status()
        
        return {'xml': response.text}

    def list_metadata_formats(self):
        """
        List available metadata formats using ListMetadataFormats verb.
        
        :return: Dict with metadata formats data
        """
        url = f"{self.base_url}?verb=ListMetadataFormats"
        response = self.session.get(url)
        response.raise_for_status()
        
        return {'xml': response.text}

def crawl_all_endpoints_xml(output_dir='data', active_only=True, max_pages=None, delay=1.0):
    """
    Convenience function to crawl all endpoints and save as XML.
    Creates a single date-based folder containing all endpoint data.
    
    :param output_dir: Output directory
    :param active_only: Only crawl active endpoints
    :param max_pages: Maximum pages per endpoint
    :param delay: Delay between requests
    :return: List of results
    """
    manager = EndpointManager()
    endpoints = manager.list_endpoints(active_only=active_only)
    
    # Create a single shared timestamp for all endpoints
    shared_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create the main date-based output directory
    date_folder = os.path.join(output_dir, shared_timestamp)
    os.makedirs(date_folder, exist_ok=True)
    
    print(f"\n{'='*60}")
    print(f"Creating shared output directory: {date_folder}")
    print(f"{'='*60}\n")
    
    results = []
    for endpoint in endpoints:
        try:
            print(f"\n{'='*60}")
            print(f"Processing: {endpoint['name']}")
            print(f"{'='*60}")
            
            client = DSpaceClient(endpoint_identifier=endpoint['id'])
            result = client.get_records_xml(
                output_dir=date_folder,
                max_pages=max_pages,
                delay=delay,
                shared_timestamp=shared_timestamp
            )
            results.append(result)
            
        except Exception as e:
            print(f"Error processing {endpoint['name']}: {e}")
            results.append({
                'success': False,
                'endpoint': endpoint['name'],
                'error': str(e)
            })
    
    return results

def crawl_all_endpoints_json(output_dir='data', active_only=True, max_pages=None, delay=1.0):
    """
    Convenience function to crawl all endpoints and save as JSON.
    Creates a single date-based folder containing all endpoint data.
    
    :param output_dir: Output directory
    :param active_only: Only crawl active endpoints
    :param max_pages: Maximum pages per endpoint
    :param delay: Delay between requests
    :return: List of results
    """
    manager = EndpointManager()
    endpoints = manager.list_endpoints(active_only=active_only)
    
    # Create a single shared timestamp for all endpoints
    shared_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create the main date-based output directory
    date_folder = os.path.join(output_dir, shared_timestamp)
    os.makedirs(date_folder, exist_ok=True)
    
    print(f"\n{'='*60}")
    print(f"Creating shared output directory: {date_folder}")
    print(f"{'='*60}\n")
    
    results = []
    for endpoint in endpoints:
        try:
            print(f"\n{'='*60}")
            print(f"Processing: {endpoint['name']}")
            print(f"{'='*60}")
            
            client = DSpaceClient(endpoint_identifier=endpoint['id'])
            result = client.get_records_json(
                output_dir=date_folder,
                max_pages=max_pages,
                delay=delay,
                shared_timestamp=shared_timestamp
            )
            results.append(result)
            
        except Exception as e:
            print(f"Error processing {endpoint['name']}: {e}")
            results.append({
                'success': False,
                'endpoint': endpoint['name'],
                'error': str(e)
            })
    
    return results