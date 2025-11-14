#!/usr/bin/env python3
"""
Main script to fetch all DSpace repository endpoints.

This script uses the DSpaceClient to retrieve data from all active
endpoints configured in the endpoints.json file.
"""

import sys
import time
import json
from datetime import datetime
from pathlib import Path

# Add parent directory to path to allow package imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from crawler.dspace_client import crawl_all_endpoints_json, crawl_all_endpoints_xml


class FetchConfig:
    """Configuration for the fetch operation."""
    
    def __init__(self, output_dir='../data', active_only=True, max_pages=None, delay=2.0):
        self.output_dir = output_dir
        self.active_only = active_only
        self.max_pages = max_pages
        self.delay = delay
    
    def display(self):
        """Display configuration settings."""
        print("Configuration:")
        print(f"  - Output directory: {self.output_dir}")
        print(f"  - Active endpoints only: {self.active_only}")
        print(f"  - Max pages per endpoint: {'Unlimited' if self.max_pages is None else self.max_pages}")
        print(f"  - Delay between requests: {self.delay}s")
        print()


class FetchResults:
    """Process and display fetch results."""
    
    def __init__(self, results, execution_time, start_time=None):
        self.results = results
        self.execution_time = execution_time
        self.start_time = start_time or datetime.now()
        self.end_time = datetime.now()
        self.successful = [r for r in results if r.get('success', False)]
        self.failed = [r for r in results if not r.get('success', False)]
    
    def display_summary(self):
        """Display results summary."""
        print("\n" + "=" * 80)
        print("FETCH SUMMARY")
        print("=" * 80)
        
        print(f"Total endpoints processed: {len(self.results)}")
        print(f"Successfully fetched: {len(self.successful)}")
        print(f"Failed: {len(self.failed)}")
        print(f"Execution time: {self.execution_time:.2f} seconds")
        
        if self.successful:
            self._display_successful()
        
        if self.failed:
            self._display_failed()
        
        print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
    
    def _display_successful(self):
        """Display successful endpoints details."""
        total_files = sum(r.get('files_created', 0) for r in self.successful)
        total_pages = sum(r.get('total_pages', 0) for r in self.successful)
        
        print(f"Total pages retrieved: {total_pages}")
        print(f"Total files created: {total_files}")
        print(f"\nSuccessful endpoints:")
        
        for result in self.successful:
            endpoint_name = result.get('endpoint', 'Unknown')
            pages = result.get('total_pages', 0)
            files = result.get('files_created', 0)
            print(f"  ✓ {endpoint_name}")
            print(f"    - Pages: {pages}, Files: {files}")
    
    def _display_failed(self):
        """Display failed endpoints details."""
        print(f"\nFailed endpoints:")
        for result in self.failed:
            endpoint_name = result.get('endpoint', 'Unknown')
            error = result.get('error', 'Unknown error')
            print(f"  ✗ {endpoint_name}: {error}")
    
    def save_summary(self, output_dir):
        """Save execution summary to a JSON file in the output directory root."""
        summary_data = {
            'execution_info': {
                'start_time': self.start_time.strftime('%Y-%m-%d %H:%M:%S'),
                'end_time': self.end_time.strftime('%Y-%m-%d %H:%M:%S'),
                'execution_time_seconds': round(self.execution_time, 2),
                'execution_time_formatted': f"{int(self.execution_time // 60)}m {int(self.execution_time % 60)}s"
            },
            'summary': {
                'total_endpoints': len(self.results),
                'successful': len(self.successful),
                'failed': len(self.failed),
                'success_rate': f"{(len(self.successful) / len(self.results) * 100):.1f}%" if self.results else "0%"
            },
            'statistics': {
                'total_pages_retrieved': sum(r.get('total_pages', 0) for r in self.successful),
                'total_files_created': sum(r.get('files_created', 0) for r in self.successful)
            },
            'successful_endpoints': [
                {
                    'name': r.get('endpoint', 'Unknown'),
                    'pages': r.get('total_pages', 0),
                    'files': r.get('files_created', 0),
                    'output_directory': r.get('output_directory', 'N/A')
                }
                for r in self.successful
            ],
            'failed_endpoints': [
                {
                    'name': r.get('endpoint', 'Unknown'),
                    'error': r.get('error', 'Unknown error'),
                    'message': r.get('message', '')
                }
                for r in self.failed
            ]
        }
        
        # Create output directory if it doesn't exist
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Save summary file
        summary_file = output_path / 'execution_summary.json'
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n📄 Execution summary saved to: {summary_file}")
        return str(summary_file)


def print_header():
    """Print script header."""
    print("=" * 80)
    print("DSPACE REPOSITORIES DATA FETCH")
    print("=" * 80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()


def fetch_json(config):
    """
    Fetch all endpoints in JSON format.
    
    Args:
        config: FetchConfig instance with fetch parameters
        
    Returns:
        tuple: (success_count, failure_count, fetch_results)
    """
    print("Fetching data in JSON format...")
    print("This provides better structured data for analysis.")
    print()
    
    start_time_dt = datetime.now()
    start_time = time.time()
    results = crawl_all_endpoints_json(
        output_dir=config.output_dir,
        active_only=config.active_only,
        max_pages=config.max_pages,
        delay=config.delay
    )
    execution_time = time.time() - start_time
    
    fetch_results = FetchResults(results, execution_time, start_time_dt)
    fetch_results.display_summary()
    
    # Save summary to output directory
    fetch_results.save_summary(config.output_dir)
    
    return len(fetch_results.successful), len(fetch_results.failed), fetch_results


def fetch_xml(config):
    """
    Fetch all endpoints in XML format.
    
    Args:
        config: FetchConfig instance with fetch parameters
        
    Returns:
        tuple: (success_count, failure_count)
    """
    print("Fetching data in XML format...")
    
    start_time = time.time()
    results = crawl_all_endpoints_xml(
        output_dir=config.output_dir,
        active_only=config.active_only,
        max_pages=config.max_pages,
        delay=config.delay
    )
    execution_time = time.time() - start_time
    
    fetch_results = FetchResults(results, execution_time)
    fetch_results.display_summary()
    
    return len(fetch_results.successful), len(fetch_results.failed)


def main():
    """Main function to fetch all repository endpoints."""
    print_header()
    
    config = FetchConfig(
        output_dir='../data',
        active_only=True,
        max_pages=None,
        delay=2.0
    )
    config.display()
    
    try:
        success_count, failure_count, fetch_results = fetch_json(config)
        return success_count, failure_count
        
    except KeyboardInterrupt:
        print("\n\nFetch interrupted by user.")
        return 0, 0
    except Exception as e:
        print(f"\nError during fetch: {e}")
        import traceback
        traceback.print_exc()
        return 0, 0


if __name__ == "__main__":
    success_count, failure_count = main()
    
    if failure_count > 0:
        print(f"\nCompleted with {failure_count} failed endpoints.")
        sys.exit(1)
    else:
        print(f"\nAll {success_count} endpoints fetched successfully!")
        sys.exit(0)