import os
import requests
import gzip
import time
from datetime import datetime, timedelta

def check_gzip_integrity(filename):
    try:
        file_size = os.path.getsize(filename)
        
        if file_size < 18:
            return False
            
        with open(filename, 'rb') as f:
            magic = f.read(2)
            if magic != b'\x1f\x8b':
                return False
            
            if file_size >= 8:
                f.seek(-8, 2)
                footer = f.read(8)
                if len(footer) != 8:
                    return False
            
        return True
    except (OSError, IOError) as e:
        print(f"File integrity check failed for {filename}: {e}")
        return False

def download_with_retry(url, filename, max_retries=3):
    for attempt in range(max_retries):
        try:
            print(f"Downloading {os.path.basename(filename)} (attempt {attempt + 1}/{max_retries})")
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()

            with open(filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            if check_gzip_integrity(filename):
                print(f"Downloaded and verified {filename}")
                return True
            else:
                print(f"Downloaded file {filename} failed integrity check, retrying...")
                if os.path.exists(filename):
                    os.remove(filename)
                continue
                
        except requests.exceptions.ReadTimeout:
            print(f"Read timeout on attempt {attempt + 1} for {url}")
            if os.path.exists(filename):
                os.remove(filename)
            if attempt < max_retries - 1:
                print(f"Retrying in 2 seconds...")
                time.sleep(2)
            continue
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {url}: {e}")
            if os.path.exists(filename):
                os.remove(filename)
            if attempt < max_retries - 1:
                print(f"Retrying in 2 seconds...")
                time.sleep(2)
            continue
        except Exception as e:
            print(f"Error saving {url}: {e}")
            if os.path.exists(filename):
                os.remove(filename)
            break
    
    return False

def download_gharchive_data(year = 2020, base_save_dir="/home/strrl/ssd/gh_data"):
    save_dir = os.path.join(base_save_dir, str(year))
    os.makedirs(save_dir, exist_ok=True)

    start_date = datetime(year, 1, 1)
    end_date = datetime(year+1, 1, 1)

    current = start_date
    total_files = 0
    downloaded_files = 0

    print(f"Start downloading {year} data to: {save_dir}")

    while current < end_date:
        month = current.strftime("%Y-%m")
        day = current.day
        hour = current.hour

        url = f"https://data.gharchive.org/{month}-{day:02d}-{hour}.json.gz"
        filename = os.path.join(save_dir, f"{month}-{day:02d}-{hour}.json.gz")

        total_files += 1

        if os.path.exists(filename):
            if check_gzip_integrity(filename):
                print(f"File already exists and is complete: {month}-{day:02d}-{hour}.json.gz")
            else:
                print(f"File exists but incomplete, re-downloading: {month}-{day:02d}-{hour}.json.gz")
                os.remove(filename)
                if download_with_retry(url, filename):
                    downloaded_files += 1
        else:
            if download_with_retry(url, filename):
                downloaded_files += 1
        
        current += timedelta(hours=1)

    print(f"\n{year} year download summary:")
    print(f"Total files: {total_files}")
    print(f"Downloaded files: {downloaded_files}")
    print(f"Already existed: {total_files - downloaded_files}")


def download_multiple_years(years, base_save_dir="/home/strrl/ssd/gh_data"):
    for year in years:
        print(f"\n{'='*50}")
        print(f"Start processing {year} data")
        print(f"{'='*50}")
        download_gharchive_data(year, base_save_dir)

if __name__ == "__main__":
    years_to_download = [2019, 2020, 2021, 2022, 2023, 2024]
    download_multiple_years(years_to_download)