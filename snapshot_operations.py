import os
import time
import requests
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional

load_dotenv()

#Sending requests to endpoint until we know its ready --> were doing this 60 times with a delay of 5 seconds in between
def poll_snapshot_status(
    snapshot_id: str, max_attempts: int = 60, delay: int = 5
) -> bool:
    api_key = os.getenv("BRIGHTDATA_API_KEY")
    progress_url = f"https://api.brightdata.com/datasets/v3/progress/{snapshot_id}"
    headers = {"Authorization": f"Bearer {api_key}"}

    for attempt in range(max_attempts):
        try:
            #Checking snapshot progress
            print(
                f"⏳ Checking snapshot progress... (attempt {attempt + 1}/{max_attempts})"
            )
            #Getting snapshot response
            response = requests.get(progress_url, headers=headers)
            response.raise_for_status()

            progress_data = response.json()

            #Status checks
            status = progress_data.get("status")

            if status == "ready":
                print("✅ Snapshot completed!")
                return True
            elif status == "failed":
                print("❌ Snapshot failed")
                return False
            elif status == "running":
                print("🔄 Still processing...")
                time.sleep(delay)
            else:
                print(f"❓ Unknown status: {status}")
                time.sleep(delay)

        except Exception as e:
            print(f"⚠️ Error checking progress: {e}")
            time.sleep(delay)

    print("⏰ Timeout waiting for snapshot completion")
    return False

    #Call this function when the snapshot is ready
def download_snapshot(
    snapshot_id: str, format: str = "json"
) -> Optional[List[Dict[Any, Any]]]:
    api_key = os.getenv("BRIGHTDATA_API_KEY")
    download_url = (
        f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}?format={format}"
    )
    headers = {"Authorization": f"Bearer {api_key}"}
    #Sending request to download the snapshot and then returns the data to us
    try:
        print("📥 Downloading snapshot data...")

        response = requests.get(download_url, headers=headers)
        response.raise_for_status()

        data = response.json()
        print(
            f"🎉 Successfully downloaded {len(data) if isinstance(data, list) else 1} items"
        )

        return data

    except Exception as e:
        print(f"❌ Error downloading snapshot: {e}")
        return None