from endpoint_manager import EndpointManager
import time
import sys


def validate_endpoints(endpoints_to_validate):
    """
    Validate a list of endpoints and update their health status.
    """
    manager = EndpointManager()

    if not endpoints_to_validate:
        print("No endpoints to validate")
        return

    print(f"Found {len(endpoints_to_validate)} endpoints to validate.")

    healthy_count = 0
    unhealthy_count = 0
    version_updates = 0

    for ep in endpoints_to_validate:
        url = ep['url']
        id = ep["id"]
        print(f"Validating {url}...")
        is_valid = manager.validate_url_health(url)
        health_status = "healthy" if is_valid else "unhealthy"
        # Use nested field for more structured health info
        manager.set_nested_field(id, 'health_status.status', health_status, allowed_fields=['health_status'])
        manager.set_nested_field(id, 'health_status.last_checked', time.time(), allowed_fields=['health_status'])

        if health_status == "healthy":
            healthy_count += 1
        else:
            unhealthy_count += 1

        # Check if version was updated
        updated_ep = manager.get_endpoint(id)
        if updated_ep and updated_ep.get('dspace_version') != ep.get('dspace_version'):
            version_updates += 1
            print(f"  Version updated to: {updated_ep['dspace_version']}")

        print(f"  Status: {health_status}")

    print("\n" + "="*50)
    print("VALIDATION SUMMARY")
    print("="*50)
    print(f"Total endpoints: {len(endpoints_to_validate)}")
    print(f"Healthy: {healthy_count}")
    print(f"Unhealthy: {unhealthy_count}")
    print(f"Version updates: {version_updates}")
    print(f"Success rate: {(healthy_count / len(endpoints_to_validate)) * 100:.1f}%")
    print("="*50)

    print("Validation complete.")


def validate_all_endpoints():
    """
    Validate all endpoints in the manager and update their 'health' status.
    """
    manager = EndpointManager()
    endpoints = manager.list_endpoints()
    validate_endpoints(endpoints)


def validate_unhealthy_endpoints():
    """
    Validate only unhealthy endpoints.
    """
    manager = EndpointManager()
    all_endpoints = manager.list_endpoints()
    unhealthy_endpoints = []

    for ep in all_endpoints:
        health_status = ep.get('health_status', {}).get('status')
        if health_status == 'unhealthy' or health_status is None:
            unhealthy_endpoints.append(ep)

    print(f"Found {len(unhealthy_endpoints)} unhealthy endpoints to re-validate.")
    validate_endpoints(unhealthy_endpoints)





if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--unhealthy":
        validate_unhealthy_endpoints()
    else:
        validate_all_endpoints()
