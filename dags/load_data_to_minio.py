from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
from minio import Minio
from io import BytesIO
import re
import time
import random

API_KEY = "1i1IV9NLGkNQm1MUHGPGtr90RfvK9l0G"
BUCKET_NAME = "thai-dataset"
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=3)
}

@dag(
    dag_id="thai_open_dataset_pipeline_parallel",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ckan", "minio", "parallel"]
)
def pipeline():

    @task
    def get_package_id_batches(batch_size=1000):
        url = "https://opend.data.go.th/get-ckan/package_list"
        headers = {'api-key': API_KEY}

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise ValueError(f"❌ Failed to fetch package list: {response.status_code}, {response.text}")

        package_ids = response.json().get("result", [])

        # แบ่งเป็น batch ละ batch_size
        return [package_ids[i:i + batch_size] for i in range(0, len(package_ids), batch_size)]

    @task
    def download_and_store_batch(batch: list):
        headers = {'api-key': API_KEY}
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)

        for pkg_id in batch:
            try:
                time.sleep(random.uniform(0.5, 2))  # ป้องกัน rate limit

                res = requests.get(f"https://opend.data.go.th/get-ckan/package_show?id={pkg_id}", headers=headers)
                if res.status_code != 200:
                    print(f"⚠️ Failed to fetch {pkg_id}: {res.status_code}")
                    continue

                pkg = res.json()["result"]
                title = pkg.get("title", pkg_id)
                folder_name = re.sub(r"[^\w\u0E00-\u0E7F]", "_", title[:100])

                for resource in pkg.get("resources", []):
                    file_url = resource.get("url")
                    if not file_url:
                        continue

                    file_res = requests.get(file_url)
                    if file_res.status_code != 200:
                        continue

                    filename = f"{resource['name'].replace(' ', '_')}.csv"
                    content = file_res.content
                    data = BytesIO(content)

                    client.put_object(
                        bucket_name=BUCKET_NAME,
                        object_name=f"{folder_name}/{filename}",
                        data=data,
                        length=len(content),
                        content_type="application/octet-stream"
                    )

                    print(f"✅ Uploaded {filename} from {pkg_id}")

            except Exception as e:
                print(f"❌ Error in package {pkg_id}: {e}")

    batches = get_package_id_batches()
    download_and_store_batch.expand(batch=batches)

pipeline()