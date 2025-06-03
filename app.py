"""
Created: 2024-01-25
Modified: 2025-06-03

Author: Hrushi 
Purpose: To bulk download data from DEA AWS bucket using python and aws

Resources: 
    Landsat Path Row Grid for Australia - https://maps.dea.ga.gov.au/#share=s-g0jnc7bO2abQ1LQlhElYownGx6C
    DEA Water Observations: https://data.dea.ga.gov.au/?prefix=derivative/ga_ls_wo_3/1-6-0/
    DEA AWS Knowledge Base: https://knowledge.dea.ga.gov.au/guides/setup/AWS/data_and_metadata/

"""


import subprocess
from datetime import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

def list_available_dates(path, row, base_url, start_date, end_date):
    """Find available dates from S3 for given path/row."""
    prefix = f"{base_url}/{path.zfill(3)}/{row.zfill(3)}/"
    dates = []

    def list_s3(prefix):
        cmd = ["aws", "s3", "ls", prefix, "--no-sign-request"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.stdout.splitlines() if result.returncode == 0 else []

    years = [line.strip().split()[-1].strip('/') for line in list_s3(prefix) if '/' in line]
    for year in years:
        if not (start_date[:4] <= year <= end_date[:4]):
            continue
        year_prefix = f"{prefix}{year}/"
        months = [line.strip().split()[-1].strip('/') for line in list_s3(year_prefix) if '/' in line]
        for month in months:
            if not (start_date[:7] <= f"{year}-{month}" <= end_date[:7]):
                continue
            month_prefix = f"{year_prefix}{month}/"
            days = [line.strip().split()[-1].strip('/') for line in list_s3(month_prefix) if '/' in line]
            for day in days:
                date_str = f"{year}-{month}-{day}"
                if start_date <= date_str <= end_date:
                    dates.append(date_str)

    return sorted(dates)


def build_s3_cmd(path, row, date, base_url, output_dir):
    """Build AWS S3 copy command for a date."""
    y, m, d = date.split("-")
    s3_path = f"{base_url}/{path.zfill(3)}/{row.zfill(3)}/{y}/{m}/{d}/"
    local_path = os.path.join(output_dir, path, row, y, m, d)
    cmd = ["aws", "s3", "cp", s3_path, local_path, "--recursive", "--no-sign-request"]
    return cmd, s3_path


def run_download(cmd, s3_path):
    print(f"📥 Downloading: {s3_path}")
    try:
        subprocess.run(cmd, check=True)
        print(f"✅ Done: {s3_path}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed: {s3_path} → {e}")


def download_dea_data(paths, rows, start_date, end_date,
                      base_url, output_dir="./dea_data",
                      multithread=False, max_workers=4):
    tasks = []
    for path in paths:
        for row in rows:
            print(f"\n🔍 Scanning available dates for path {path}, row {row}...")
            dates = list_available_dates(path, row, base_url, start_date, end_date)
            if not dates:
                print(f"⚠️ No data found for {path}/{row}")
                continue
            short_dates = [datetime.strptime(d, "%Y-%m-%d").strftime("%#d %b") for d in dates]
            print(f"📅 Found {len(dates)} dates for {path}/{row}: {', '.join(short_dates)}")
            for date in dates:
                cmd, s3_path = build_s3_cmd(path, row, date, base_url, output_dir)
                tasks.append((cmd, s3_path))

    print(f"\n🚀 Starting downloads: {len(tasks)} total\n")

    if multithread:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(run_download, cmd, s3_path): s3_path for cmd, s3_path in tasks}
            for future in as_completed(futures):
                future.result()
    else:
        for i, (cmd, s3_path) in enumerate(tasks, 1):
            print(f"🔢 [{i}/{len(tasks)}]")
            run_download(cmd, s3_path)


# Usage
            
if __name__ == "__main__":

    paths = ['100', '101', '102']
    rows = ['080']
    start_date = "2025-04-25"
    end_date = "2025-06-25"
    base_url = "s3://dea-public-data/derivative/ga_ls_wo_3/1-6-0"

    download_dea_data(
        paths=paths,
        rows=rows,
        start_date=start_date,
        end_date=end_date,
        base_url=base_url,
        output_dir="./dea_data",
        multithread=True,
        max_workers=4
    )


