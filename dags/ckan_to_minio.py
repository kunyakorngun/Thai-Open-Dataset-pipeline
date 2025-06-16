from airflow.decorators import dag, task
from datetime import datetime
from minio import Minio
from io import BytesIO
import requests
import json
import chardet
import logging

@dag(
    dag_id="Thai_Open_Dataset_Pipeline_all_packages",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=None,
)
def Thai_Open_Dataset_Pipeline_all_packages():

    @task
    def fetch_and_upload():
        url = "https://opend.data.go.th/get-ckan/package_list"
        url_search_package_by_id = "https://opend.data.go.th/get-ckan/package_show?id="
        headers = {
            'api-key': 'YTLKiuf3efdlDBYELchMPqXpZ8pnjLCK'
        }

        response = requests.get(url, headers=headers)
        json_data = response.json()
        package_ids = json_data['result']

        # Setup MinIO client
        client = Minio(
            "minio:9000",
            access_key="minio",
            secret_key="minio123",
            secure=False
        )

        bucket_name = "dataset-bucket"
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)

        for pkg_id in package_ids:
            try:
                package_response = requests.get(url_search_package_by_id + pkg_id, headers=headers)
                json_package = package_response.json()
                resources = json_package['result'].get('resources', [])

                for res in resources:
                    file_url = res.get('url')
                    if not file_url:
                        continue

                    filetype = file_url.split('/')[-1].split('.')[-1].lower()
                    filename = f"{res['name'].replace(' ', '_')}.{filetype}"
                    object_name = filename  # สำหรับใช้กับ MinIO

                    print(f"📥 Downloading: {filename}")

                    file_response = requests.get(file_url)
                    if file_response.status_code != 200:
                        print(f"❌ Failed to download: {file_url}")
                        continue

                    file_content = file_response.content

                    # แปลง encoding ถ้าเป็น CSV
                    if filetype == "csv":
                        detected = chardet.detect(file_content)
                        encoding = detected.get("encoding", "utf-8")

                        print(f"🔍 Detected encoding for {filename}: {encoding}")
                        try:
                            text = file_content.decode(encoding)
                            utf8_content = text.encode("utf-8-sig")
                            data = BytesIO(utf8_content)
                            size = len(utf8_content)
                        except Exception as e:
                            print(f"⚠️ Failed to convert {filename} to UTF-8: {e}")
                            continue
                    else:
                        data = BytesIO(file_content)
                        size = len(file_content)

                    # Upload เข้า MinIO
                    result = client.put_object(
                        bucket_name=bucket_name,
                        object_name=object_name,
                        data=data,
                        length=size,
                        content_type="application/octet-stream"
                    )
                    print(f"✅ Uploaded: {result.object_name} (etag: {result.etag})")

            except Exception as e:
                print(f"💥 Failed on package {pkg_id}: {e}")

    fetch_and_upload()

dag = Thai_Open_Dataset_Pipeline_all_packages()
