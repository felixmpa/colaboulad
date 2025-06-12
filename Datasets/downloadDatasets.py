#!/usr/bin/env python3
import argparse, pathlib, requests, sys, zipfile
from tqdm import tqdm  # pip install tqdm

URL = "https://analyse.kmi.open.ac.uk/open-dataset/download"

def download(dest_folder: pathlib.Path, extract=True):
    dest_folder.mkdir(parents=True, exist_ok=True)
    out_file = dest_folder / "oulad.zip"
    
    # Download if not exists
    if not out_file.exists():
        print(f"Downloading OULAD dataset...")
        with requests.get(URL, stream=True, allow_redirects=True) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            with open(out_file, "wb") as f, tqdm(
                total=total, unit="B", unit_scale=True, desc=out_file.name
            ) as bar:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    bar.update(len(chunk))
        print(f"✓ Saved to {out_file.resolve()}")
    else:
        print(f"✓ Using existing file: {out_file.resolve()}")
    
    # Extract if requested
    if extract:
        print(f"Extracting files...")
        with zipfile.ZipFile(out_file, 'r') as zip_ref:
            zip_ref.extractall(dest_folder)
        print(f"✓ Extracted to {dest_folder.resolve()}")
        
        # List extracted files
        csv_files = list(dest_folder.glob("*.csv"))
        if csv_files:
            print(f"\nExtracted {len(csv_files)} CSV files:")
            for csv_file in sorted(csv_files):
                print(f"  - {csv_file.name}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download and extract the OULAD dataset"
    )
    parser.add_argument("folder", nargs="?", default=".", help="destination directory (default: current)")
    parser.add_argument("--no-extract", action="store_true", help="don't extract the ZIP file")
    args = parser.parse_args()
    download(pathlib.Path(args.folder), extract=not args.no_extract)
