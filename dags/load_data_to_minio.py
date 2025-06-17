# นำเข้าโมดูลที่จำเป็นจาก Airflow, datetime, requests และ MinIO
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
from minio import Minio
from io import BytesIO
import re
import time
import random

# กำหนดค่าคงที่สำหรับการเชื่อมต่อ API และ MinIO
API_KEY = "lbQ9Xdt6OvUbdJQBQBcOyn7rLIDnrPPB"
BUCKET_NAME = "thai-dataset"
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"

# ค่าตั้งต้นเมื่อ task ล้มเหลว จะพยายามใหม่ 3 ครั้ง หน่วงแต่ละครั้ง 3 นาที
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=3)
}

# กำหนด DAG 
@dag(
    dag_id="thai_open_dataset_pipeline_parallel",  # ชื่อของ DAG
    schedule=None,  # ไม่มีการตั้งเวลารันอัตโนมัติ
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ckan", "minio", "parallel"]
)
def pipeline():

    # Task: ดึงรายการ package_id ทั้งหมดจาก API แล้วแบ่งเป็นกลุ่มย่อย (batch)
    @task
    def get_package_id_batches(batch_size=1000):
        url = "https://opend.data.go.th/get-ckan/package_list"
        headers = {'api-key': API_KEY}

        max_retries = 5 # จํานวนครั้งที่จะการพยายามส่ง HTTP request เพื่อดึงข้อมูลจาก API หลายครั้ง (สูงสุด 5 ครั้ง) เวลาเจอปัญหา 429
        for attempt in range(max_retries):
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                # ดึงผลลัพธ์ทั้งหมดแล้วแบ่ง batch ละ batch_size ชุด
                package_ids = response.json().get("result", [])
                return [package_ids[i:i + batch_size] for i in range(0, len(package_ids), batch_size)]
            elif response.status_code == 429:
                # ถ้าเจอ rate limit ให้รอแบบ exponential backoff
                wait_time = 2 ** attempt
                print(f"API rate limit hit (429). Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                raise ValueError(f"Failed to fetch package list: {response.status_code}, {response.text}")

        raise ValueError("Exceeded maximum retries due to API rate limit (429)")

    # Task: ดาวน์โหลดและจัดเก็บข้อมูลจากแต่ละ package ลง MinIO
    @task
    def download_and_store_batch(batch: list):
        headers = {'api-key': API_KEY}
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

        # ตรวจสอบว่า bucket มีอยู่หรือยัง ถ้าไม่มีก็สร้าง
        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)

        # วนลูปแต่ละ package_id ใน batch
        for pkg_id in batch:
            try:
                time.sleep(random.uniform(0.5, 2))  # ป้องกันไม่ให้ยิง request รัวเกินไป ให้รอเวลา

                # ดึงข้อมูล package โดยมี retry ถ้าเจอ rate limit
                for attempt in range(3):
                    res = requests.get(f"https://opend.data.go.th/get-ckan/package_show?id={pkg_id}", headers=headers)
                    if res.status_code == 200:
                        break
                    elif res.status_code == 429:
                        wait_time = 2 ** attempt
                        print(f"Rate limited on {pkg_id}. Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        print(f"Failed to fetch {pkg_id}: {res.status_code}")
                        res = None
                        break

                if not res or res.status_code != 200:
                    continue  # ข้าม package นี้ถ้าโหลดไม่สำเร็จ

                pkg = res.json()["result"]
                title = pkg.get("title", pkg_id) #เอาชื่อจาก title มาตั้งชื่อโฟลเดอร์

                # ตั้งชื่อโฟลเดอร์ตาม title ของ package (แทนที่อักขระพิเศษทั้งหมด)
                folder_name = re.sub(r"[^\w\u0E00-\u0E7F]", "_", title[:100])

                # วนลูป resource ที่อยู่ใน package
                for resource in pkg.get("resources", []):
                    file_url = resource.get("url")
                    if not file_url:
                        continue

                    try:
                        file_res = requests.get(file_url)
                        if file_res.status_code != 200:
                            continue

                        # กำหนดชื่อไฟล์ตามชื่อ resource
                        filename = f"{resource['name'].replace(' ', '_')}.csv"
                        content = file_res.content
                        data = BytesIO(content)

                        # อัปโหลดไฟล์เข้า MinIO ภายใต้ path: {folder_name}/{filename}
                        client.put_object(
                            bucket_name=BUCKET_NAME,
                            object_name=f"{folder_name}/{filename}", #กำหนด path,ชื่อไฟล์ข้างในminio
                            data=data, #ไฟล์
                            length=len(content), #ขนาดไฟล์
                            content_type="application/octet-stream" #MIME type สำหรับบอกว่าเป็นไฟล์ binary ทั่วไป
                        )
                        print(f"Uploaded {filename} from {pkg_id}")
                    except Exception as e:
                        print(f"Error downloading/uploading resource from {pkg_id}: {e}")

            except Exception as e:
                print(f"Error in package {pkg_id}: {e}")

    # เรียกใช้ tasks และขยาย task download ให้ทำงานแบบ parallel ต่อ batch
    batches = get_package_id_batches()
    download_and_store_batch.expand(batch=batches)
    
pipeline()
