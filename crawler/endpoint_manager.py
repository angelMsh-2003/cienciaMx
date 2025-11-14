import json
import os
import requests
from datetime import datetime, timezone
import urllib3

# Disable SSL warnings when verify=False is used
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class EndpointManager:
    _instance = None

    def __new__(cls, file_path='endpoints.json'):
        if cls._instance is None:
            cls._instance = super(EndpointManager, cls).__new__(cls)
        return cls._instance
    """
    Manages a list of endpoints stored in a JSON file.
    Provides CRUD operations for endpoints.
    """

    def __init__(self, file_path='endpoints.json'):
        """
        Initialize the EndpointManager with the path to the JSON file.
        This is called only once due to singleton pattern.

        :param file_path: Path to the JSON file (default: 'endpoints.json')
        """
        if not hasattr(self, 'initialized'):
            self.file_path = file_path
            self.endpoints = []
            self.next_id = 1
            self.load_endpoints()
            self.initialized = True

    def load_endpoints(self):
        """
        Load endpoints from the JSON file.
        If the file doesn't exist, initialize with an empty list.
        """
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.endpoints = data.get('endpoints', [])
                    # Set next_id to max id + 1
                    if self.endpoints:
                        self.next_id = max(ep.get('id', 0) for ep in self.endpoints) + 1
                    else:
                        self.next_id = 1
            except (json.JSONDecodeError, IOError) as e:
                print(f"Error loading endpoints: {e}")
                self.endpoints = []
                self.next_id = 1
        else:
            self.endpoints = []
            self.next_id = 1

    def save_endpoints(self):
        """
        Save the current endpoints list to the JSON file.
        """
        try:
            with open(self.file_path, 'w', encoding='utf-8') as f:
                json.dump({'endpoints': self.endpoints}, f, indent=2, ensure_ascii=False)
        except IOError as e:
            print(f"Error saving endpoints: {e}")

    def add_endpoint(self, url, name, description=None, active=True, dspace_version=None, number_of_items=0):
        """
        Add a new endpoint. Checks for unique URL.

        :param url: The endpoint URL
        :param name: The endpoint name
        :param description: Optional description
        :param active: Whether the endpoint is active (default: True)
        :param dspace_version: DSpace version (e.g., "6.2", "7.0")
        :param number_of_items: Number of items in the repository (default: 0)
        :return: The new endpoint ID if added, False if URL already exists
        """
        if any(ep['url'] == url for ep in self.endpoints):
            return False
        endpoint = {
            'url': url,
            'name': name,
            'description': description,
            'active': active,
            'added_at': None,
            'last_crawled': None,
            'error_count': 0,
            'id': self.next_id,
            'dspace_version': dspace_version,
            'number_of_items': number_of_items,
            'health_status': {
                'status': 'unknown',
                'last_checked': None
            }
        }
        self.endpoints.append(endpoint)
        self.next_id += 1
        self.save_endpoints()
        return endpoint['id']

    def remove_endpoint(self, identifier):
        """
        Remove an endpoint by ID or URL.

        :param identifier: The endpoint ID (int) or URL (str) to remove
        :return: True if removed, False if not found
        """
        for i, ep in enumerate(self.endpoints):
            if ep.get('id') == identifier or ep['url'] == identifier:
                del self.endpoints[i]
                self.save_endpoints()
                return True
        return False

    def update_endpoint(self, identifier, **kwargs):
        """
        Update an endpoint's fields by ID or URL.

        :param identifier: The endpoint ID (int) or URL (str) to update
        :param kwargs: Fields to update (e.g., name='new_name', active=False)
        :return: True if updated, False if not found
        """
        for ep in self.endpoints:
            if ep.get('id') == identifier or ep['url'] == identifier:
                for key, value in kwargs.items():
                    if key in ep:
                        ep[key] = value
                self.save_endpoints()
                return True
        return False

    def get_endpoint(self, identifier):
        """
        Get an endpoint by ID or URL.

        :param identifier: The endpoint ID (int) or URL (str)
        :return: The endpoint dict or None if not found
        """
        for ep in self.endpoints:
            if ep.get('id') == identifier or ep['url'] == identifier:
                return ep
        return None

    def list_endpoints(self, active_only=False):
        """
        List all endpoints or only active ones.

        :param active_only: If True, return only active endpoints
        :return: List of endpoint dicts
        """
        if active_only:
            return [ep for ep in self.endpoints if ep.get('active', False)]
        return self.endpoints

    def activate_endpoint(self, identifier):
        """
        Activate an endpoint by ID or URL.

        :param identifier: The endpoint ID (int) or URL (str)
        :return: True if activated, False if not found
        """
        return self.update_endpoint(identifier, active=True)

    def deactivate_endpoint(self, identifier):
        """
        Deactivate an endpoint by ID or URL.

        :param identifier: The endpoint ID (int) or URL (str)
        :return: True if deactivated, False if not found
        """
        return self.update_endpoint(identifier, active=False)

    def increment_error_count(self, identifier):
        """
        Increment the error count for an endpoint.

        :param identifier: The endpoint ID (int) or URL (str)
        :return: True if incremented, False if not found
        """
        ep = self.get_endpoint(identifier)
        if ep:
            ep['error_count'] = ep.get('error_count', 0) + 1
            self.save_endpoints()
            return True
        return False

    def update_last_crawled(self, identifier):
        """
        Update the last_crawled timestamp for an endpoint.

        :param identifier: The endpoint ID (int) or URL (str)
        :return: True if updated, False if not found
        """
        return self.update_endpoint(identifier, last_crawled=datetime.now(timezone.utc).isoformat())

    def set_nested_field(self, identifier, field_path, value, allowed_fields=None):
        """
        Set a nested field in an endpoint using dot notation (e.g., 'health_status.code').
        Creates intermediate dictionaries if they don't exist.

        :param identifier: The endpoint ID (int) or URL (str)
        :param field_path: Dot-separated field path (e.g., 'health_status.code')
        :param value: Value to set
        :param allowed_fields: List of allowed top-level fields to prevent arbitrary creation
        :return: True if set, False if not found or field not allowed
        """
        ep = self.get_endpoint(identifier)
        if not ep:
            return False

        parts = field_path.split('.')
        top_field = parts[0]

        # Security check: only allow certain top-level fields
        if allowed_fields and top_field not in allowed_fields:
            return False

        current = ep
        for part in parts[:-1]:
            if part not in current or not isinstance(current[part], dict):
                current[part] = {}
            current = current[part]

        current[parts[-1]] = value
        self.save_endpoints()
        return True

    def validate_url_health(self, url, timeout=10):
        """
        Validate if a URL is reachable and returns a valid response.
        For DSpace endpoints, check health/status endpoints based on version.
        If version is unknown, try both DSpace 7 and 5/6 health endpoints and update version if successful.

        :param url: The URL to validate
        :param timeout: Timeout in seconds (default: 10)
        :return: True if valid, False otherwise
        """
        # Store original URL for endpoint lookup
        original_url = url
        
        # Add OAI-PMH Identify verb to test the endpoint
        test_url = url + "?verb=Identify"

        # Use a browser-like User-Agent to avoid being blocked by servers
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        # First, try basic URL validation with OAI-PMH Identify
        # Disable SSL verification to handle self-signed or invalid certificates
        try:
            response = requests.get(test_url, timeout=timeout, verify=False, headers=headers)
            if response.status_code == 200:
                return True
        except requests.RequestException:
            pass  # Continue to try other methods

        # If basic check failed, try DSpace-specific health endpoints
        ep = self.get_endpoint(original_url)
        if ep:
            version = ep.get('dspace_version')
            if version and not version.startswith('Unknown'):
                if version.startswith('7'):
                    # DSpace 7 uses /server/oai/request, replace with actuator health endpoint
                    health_url = original_url.replace('/server/oai/request', '/server/actuator/health')
                elif version.startswith(('5', '6')):
                    # DSpace 5/6 uses /oai/request, replace with rest status endpoint
                    health_url = original_url.replace('/oai/request', '/rest/status')
                else:
                    return False  # Basic check already failed

                try:
                    health_response = requests.get(health_url, timeout=timeout, verify=False, headers=headers)
                    return health_response.status_code == 200
                except requests.RequestException:
                    return False
            else:
                # Version unknown, try DSpace 7 first
                # Check if it's already a DSpace 7 URL pattern
                if '/server/oai/request' in original_url:
                    health_url_7 = original_url.replace('/server/oai/request', '/server/actuator/health')
                else:
                    health_url_7 = original_url.replace('/oai/request', '/server/actuator/health')
                
                try:
                    health_response = requests.get(health_url_7, timeout=timeout, verify=False, headers=headers)
                    if health_response.status_code == 200:
                        self.update_endpoint(original_url, dspace_version='7.x')
                        return True
                except requests.RequestException:
                    pass

                # Try DSpace 5/6
                health_url_56 = original_url.replace('/oai/request', '/rest/status').replace('/server/oai/request', '/rest/status')
                try:
                    health_response = requests.get(health_url_56, timeout=timeout, verify=False, headers=headers)
                    if health_response.status_code == 200:
                        self.update_endpoint(original_url, dspace_version='6.x')
                        return True
                except requests.RequestException:
                    pass

                return False  # Neither health endpoint worked

        # If no endpoint found in database, basic check result stands
        return False
    



