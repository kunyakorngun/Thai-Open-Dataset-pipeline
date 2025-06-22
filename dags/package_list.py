import requests
import json

API_KEY = "SVvAl1yR9ACOaVTY09V3lyuehgHFLnMe"
BATCH_SIZE = 1000
OUTPUT_FILE = "./package_batches.json"

def fetch_and_save_package_batches():
    url = "https://opend.data.go.th/get-ckan/package_list"
    headers = {'api-key': API_KEY}

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise ValueError(f"Failed to fetch package list: {response.status_code}, {response.text}")

    # package_ids = response.json().get("result", [])
    # batches = [package_ids[i:i + BATCH_SIZE] for i in range(0, len(package_ids), BATCH_SIZE)]

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        # json.dump(batches, f, ensure_ascii=False, indent=2)
        json.dump(response.json(), f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    fetch_and_save_package_batches()
