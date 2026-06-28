import os
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ==========================================================
# Configuration
# ==========================================================

BASE_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

DATA_DIR = "data"

YEAR = "2023"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/137.0 Safari/537.36"
    )
}

EXPECTED_FILES = [
    "taxi_zone_lookup.csv",
]

EXPECTED_FILES.extend(
    [
        f"yellow_tripdata_{YEAR}-{month:02d}.parquet"
        for month in range(1, 13)
    ]
)


# ==========================================================
# Helper Functions
# ==========================================================

def all_files_exist():
    """Check whether all required datasets already exist."""
    return all(
        os.path.exists(os.path.join(DATA_DIR, file))
        for file in EXPECTED_FILES
    )


def download(url, filename):
    """Download a file with retries."""

    filepath = os.path.join(DATA_DIR, filename)

    if os.path.exists(filepath):
        print(f"✓ Already exists : {filename}")
        return "skipped"

    print(f"↓ Downloading    : {filename}")

    for attempt in range(1, 4):

        try:

            response = requests.get(
                url,
                headers=HEADERS,
                stream=True,
                timeout=300,
            )

            response.raise_for_status()

            with open(filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

            print(f"✓ Downloaded    : {filename}")
            return "downloaded"

        except Exception as e:

            print(f"Retry {attempt}/3 -> {e}")

            time.sleep(2)

    print(f"✗ Failed        : {filename}")

    return "failed"


# ==========================================================
# Main
# ==========================================================

def main():

    os.makedirs(DATA_DIR, exist_ok=True)

    print("=" * 70)
    print("NYC Taxi Data Downloader")
    print("=" * 70)

    # ------------------------------------------------------

    if all_files_exist():

        print("All required datasets already exist.")
        print("Skipping download.")

        print("=" * 70)

        return

    print("Reading NYC TLC website...")

    response = requests.get(
        BASE_URL,
        headers=HEADERS,
        timeout=30,
    )

    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    downloaded = 0
    skipped = 0
    failed = 0

    links = soup.find_all("a")

    processed = set()

    for link in links:

        href = link.get("href")

        if not href:
            continue

        url = urljoin(BASE_URL, href)

        filename = url.split("/")[-1]

        if (
            filename.startswith(f"yellow_tripdata_{YEAR}")
            and filename.endswith(".parquet")
        ):

            if filename in processed:
                continue

            processed.add(filename)

            status = download(url, filename)

            if status == "downloaded":
                downloaded += 1
            elif status == "skipped":
                skipped += 1
            else:
                failed += 1

        elif filename == "taxi_zone_lookup.csv":

            if filename in processed:
                continue

            processed.add(filename)

            status = download(url, filename)

            if status == "downloaded":
                downloaded += 1
            elif status == "skipped":
                skipped += 1
            else:
                failed += 1

    print()

    print("=" * 70)
    print("Download Summary")
    print("=" * 70)
    print(f"Downloaded : {downloaded}")
    print(f"Skipped    : {skipped}")
    print(f"Failed     : {failed}")
    print("=" * 70)

    missing = [
        file
        for file in EXPECTED_FILES
        if not os.path.exists(os.path.join(DATA_DIR, file))
    ]

    if missing:

        print("\nMissing files:")

        for file in missing:
            print(f"  - {file}")

    else:

        print("\n✓ Dataset is complete and ready for ingestion.")


if __name__ == "__main__":
    main()