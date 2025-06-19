from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
from minio import Minio
from io import BytesIO
import requests
import json
import os

# --- CONFIG ---
API_KEY = "Np76FNlQTpjMNoUPFJkJc0Nf7cv63vhd"
BUCKET_NAME = "dataset-bucket2"
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BATCH_LIST_FILE = "/opt/airflow/dags/package_batches.json"

# total = 32473
# batch_size = 50
total = 100
batch_size = 20

# for start in range(0, total, batch_size):
#     end = min(start + batch_size - 1, total - 1)
#     # print(f"Trigger DAG with START_INDEX={start}, END_INDEX={end}")
#     START_INDEX = start
#     END_INDEX = end

# START_INDEX = 0
# END_INDEX = 32473

@dag(
    dag_id="thai_open_dataset_pipeline_modular",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ckan", "minio", "modular"]
)
def pipeline():

    @task()
    def get_package_ids():
        for start in range(0, total, batch_size):
            end = min(start + batch_size - 1, total - 1)
            START_INDEX = start
            END_INDEX = end
            
        with open(BATCH_LIST_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("result", [])[START_INDEX:END_INDEX + 1]

    @task()
    def fetch_package_detail(package_id: str):
        headers = {"api-key": API_KEY}
        url = f"https://data.go.th/api/3/action/package_show?id={package_id}"
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code != 200:
            raise ValueError(f"Failed to fetch: {package_id}")
        pkg = res.json()["result"]
        return {
            "package_id": package_id,
            "title": pkg.get("title", package_id),
            "resources": pkg.get("resources", [])[:5], #ตัดเอาเฉพาะ 5 รายการแรก จาก list นั้นเท่านั้นตามที่กำหนดให้เอา resources ได้ไม่เกิน 5
            #"last_modified": pkg.get("last_modified", []) #ถ้าไม่มีให้คืนเป็น list ว่าง
        }

    @task()
    def download_and_upload_resources(pkg_data: dict):
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

        title = pkg_data["title"].replace("/", "_").replace("\\", "_")
        #สำหรับ download ไฟล์จาก resources
        for resource in pkg_data["resources"]:
            
            if "original_url" in resource:
                download_url = resource["original_url"]
            elif "url" in resource:
                download_url = resource["url"]
            else:
                download_url = None

            if not download_url:
                print(f"❌ No downloadable URL for resource: {resource}")
                continue

            #ไปเอา original_url หรือ url เพื่อมาโหลด มีอันไหนเอาอันนั้น
            # original_url = resource.get("original_url") or resource.get("")
            # # if not original_url:
            # #     continue
            
            try:
                r = requests.get(download_url, timeout=20)
                if r.status_code != 200:
                    print(f"❌ Failed to download: {download_url}")
                    continue 

                file_data = r.content
                name = resource.get("name", resource["id"]).strip().replace("/", "_")
                ext = os.path.splitext(download_url)[-1] or ".csv"
                filename = f"{name}{ext}"
                object_path = f"{title}/{filename}"

                minio_client.put_object(
                    bucket_name=BUCKET_NAME,
                    object_name=object_path,
                    data=BytesIO(file_data),
                    length=len(file_data),
                    content_type="application/octet-stream"
                )

                print(f"✅ Uploaded: {object_path}")

            except Exception as e:
                print(f"❌ Error downloading/uploading resource: {download_url} - {e}")


    # ==== DAG FLOW ====
    package_ids = get_package_ids()
    package_details = fetch_package_detail.expand(package_id=package_ids)
    download_and_upload_resources.expand(pkg_data=package_details)


dag = pipeline()
