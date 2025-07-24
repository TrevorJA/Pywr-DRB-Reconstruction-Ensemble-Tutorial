"""
Zenodo Download Script
Downloads files from https://zenodo.org/records/15101164
"""

import subprocess
import requests
import shutil
from pathlib import Path

def download_zenodo_record(record_id, 
                           download_dir, 
                           target_filename=None):
    """
    Download files from Zenodo record 15101164
    
    Args:
        target_filename: Specific file to download (None = download all)
    """
    
    print(f"Downloading from Zenodo record {record_id}...")
    
    # Get record metadata
    url = f"https://zenodo.org/api/records/{record_id}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        files = data['files']
        
        # Filter files if specific filename requested
        if target_filename:
            files = [f for f in files if f['key'] == target_filename]
            if not files:
                available = [f['key'] for f in data['files']]
                print(f"File '{target_filename}' not found.")
                print(f"Available files: {available}")
                return
        
        print(f"Found {len(files)} file(s) to download")
        
    except requests.RequestException as e:
        print(f"Error fetching record metadata: {e}")
        return
    
    # Create download directory
    Path(download_dir).mkdir(parents=True, exist_ok=True)
    
    # Download using wget or requests
    if shutil.which('wget'):
        print("Using wget for download...")
        for file_info in files:
            file_url = file_info['links']['self']
            file_name = file_info['key']  # This has the real filename
            output_path = Path(download_dir) / file_name
            print(f"Downloading {file_name}...")
            
            subprocess.run([
                'wget', 
                '-O', str(output_path),  # Specify output filename
                '-c', '-t', '3', '--timeout=30',
                file_url
            ], check=True)
    else:
        print("Using Python requests...")
        for file_info in files:
            file_url = file_info['links']['self']
            file_name = file_info['key']
            print(f"Downloading {file_name}...")
            
            with requests.get(file_url, stream=True) as r:
                r.raise_for_status()
                filepath = Path(download_dir) / file_name
                with open(filepath, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
    
    print(f"Download complete. Files saved to {download_dir}/")


if __name__ == "__main__":

    record_id = "15101164"
    download_dir = "./"
    target_filename = "drb_historic_streamflow_ensemble_data.zip"

    download_zenodo_record(record_id, download_dir, target_filename)