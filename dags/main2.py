from airflow.decorators import dag, task
from datetime import datetime
from minio import Minio
from io import BytesIO
import requests
import json
import chardet

@dag(
    dag_id="Thai_Open_Dataset_Pipeline_test",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=None,
)
def Thai_Open_Dataset_Pipeline_test():

    @task
    def fetch_and_upload():
        url = "https://opend.data.go.th/get-ckan/package_list"
        url_search_package_by_id = "https://opend.data.go.th/get-ckan/package_show?id="

        headers = {
            'api-key': '1i1IV9NLGkNQm1MUHGPGtr90RfvK9l0G'
        }

        response = requests.get(url, headers=headers)
        json_data = response.json()

        number_end = 10

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

        for i in range(number_end):
            pkg_id = json_data['result'][i]
            package_response = requests.get(url_search_package_by_id + pkg_id, headers=headers)
            json_package = package_response.json()
            resources = json_package['result'].get('resources', [])

            for res in resources:
                file_url = res['url']
                filetype = file_url.split('/')[-1].split('.')[-1].lower()
                filename = f"{res['name'].replace(' ', '_')}.{filetype}"
                object_name = filename

                file_response = requests.get(file_url)
                if file_response.status_code == 200:
                    file_content = file_response.content

                    # 🔁 แปลง encoding เฉพาะไฟล์ .csv เท่านั้น
                    if filetype == "csv":
                        # ตรวจสอบ encoding
                        detected = chardet.detect(file_content)
                        encoding = detected.get("encoding", "utf-8")

                        print(f"📌 Detected encoding for {filename}: {encoding}")
                    
                        try:
                            text = file_content.decode(encoding)
                            utf8_content = text.encode("utf-8-sig")  # ✅ บางโปรแกรมไทยต้องการ BOM ด้วย
                            data = BytesIO(utf8_content)
                            size = len(utf8_content)
                        except Exception as e:
                            print(f"❌ Failed to convert {filename} to UTF-8: {e}")
                            continue
                    else:
                        data = BytesIO(file_content)
                        size = len(file_content)

                    # ✅ Upload เข้า MinIO
                    result = client.put_object(
                        bucket_name=bucket_name,
                        object_name=object_name,
                        data=data,
                        length=size,
                        content_type="application/octet-stream"
                    )
                    print(f"✅ Uploaded {result.object_name} (etag: {result.etag})")
                else:
                    print(f"❌ Failed to download {file_url}")
                

    fetch_and_upload()

dag = Thai_Open_Dataset_Pipeline_test()
