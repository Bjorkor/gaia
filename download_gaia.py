#!/home/bjorkor/projects/gaia/venv/bin/python
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import threading
from queue import Queue
from tqdm import tqdm
import signal
import sys
from requests.exceptions import ChunkedEncodingError

# Base URL of the webpage
base_url = "http://cdn.gea.esac.esa.int/Gaia/gdr1/gaia_source/fits/"

# Directory to save downloaded files
local_base_dir = "/mnt/nas/gaia_data/gdr1/gaia_source/fits"

# Number of threads
num_threads = 100

# Flag to indicate if the script should exit early
exit_flag = threading.Event()

# Number of retries for downloads
max_retries = 3

def signal_handler(sig, frame):
    print("Signal received, exiting gracefully...")
    exit_flag.set()

def download_file(url, local_dir):
    if exit_flag.is_set():
        return
    
    local_filename = os.path.join(local_dir, os.path.basename(url))
    
    # Check if the file already exists
    if os.path.exists(local_filename):
        local_file_size = os.path.getsize(local_filename)
        
        response = requests.head(url)
        response.raise_for_status()
        remote_file_size = int(response.headers.get('Content-Length', 0))
        
        if local_file_size == remote_file_size:
            print(f"File {local_filename} already exists and is complete. Skipping download.")
            return
        else:
            print(f"File {local_filename} exists but is incomplete. Redownloading.")
    
    retries = 0
    while retries < max_retries:
        try:
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with open(local_filename, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if exit_flag.is_set():
                            print("Download interrupted, exiting...")
                            return
                        f.write(chunk)
            break  # Break the loop if the download is successful
        except (ChunkedEncodingError, requests.exceptions.ConnectionError) as e:
            retries += 1
            print(f"Error downloading {url}: {e}. Retrying {retries}/{max_retries}...")
            if retries >= max_retries:
                print(f"Failed to download {url} after {max_retries} attempts.")
                return

def worker(queue, progress_bar):
    while not exit_flag.is_set():
        try:
            url = queue.get(timeout=1)
        except:
            continue
        
        if url is None:
            break
        download_file(url, local_base_dir)
        progress_bar.update(1)
        queue.task_done()

def fetch_fits_files(url):
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    fits_files = [urljoin(url, link.get('href')) for link in soup.find_all('a') if link.get('href').endswith('.fits')]
    return fits_files

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)

    fits_files = fetch_fits_files(base_url)
    total_files = len(fits_files)

    print(f"Found {total_files} .fits files to download.")

    queue = Queue()

    with tqdm(total=total_files, desc="Downloading Files", unit="file") as progress_bar:
        threads = []
        for _ in range(num_threads):
            t = threading.Thread(target=worker, args=(queue, progress_bar))
            t.start()
            threads.append(t)

        for file_url in fits_files:
            if exit_flag.is_set():
                break
            queue.put(file_url)

        queue.join()

        for _ in range(num_threads):
            queue.put(None)
        for t in threads:
            t.join()

    print("Script exited gracefully.")
