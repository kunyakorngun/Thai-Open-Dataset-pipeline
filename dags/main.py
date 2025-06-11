from airflow.decorators import dag, task, task_group
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from minio import Minio
from minio.error import S3Error
import boto3
import requests
import json
import pandas as pd
import os 


url = "https://opend.data.go.th/get-ckan/package_list" #urlที่ลิส package ทั้งหมด
url_search_package_by_id = "https://opend.data.go.th/get-ckan/package_show?id=" #urlไว้เลือกpackageแบบเป็นราย id จากที่เราลิสทั้งหมด

payload = {}
headers = {
  'api-key': '1i1IV9NLGkNQm1MUHGPGtr90RfvK9l0G' #กำหนด header ตามที่ ckan กำหนด
}

response = requests.request("GET", url, headers=headers, data=payload) #ไป get ข้อมูล

json_data = json.loads(response.text)  #เอาข้อมูลที่ get มาแปลงเป็น json
# number_end = len(json_data['result'])
number_end = 6 #กําหนดจํานวน package ที่จะดาวน์โหลด
for i in range(0, number_end):
    package = requests.request("GET", url_search_package_by_id + json_data['result'][i], headers=headers, data=payload) #เรียกใช้ url ที่เอาไว้เลือก package เป็นราย id แล้วใส่ id ที่ลิสไว้เพื่อดาวน์โหลดทั้งหมด
    # print(json_data['result'][0])
    json_package = json.loads(package.text) #GET(id package ทั้งหมด)เสร็จแล้วดาวน์โหลดเป็น txt
    package_name = json_package['result']['title'] #ดึงชื่อ package ออกมา
    for l in json_package['result']['resources']:
        filetype = l['url'].split('/')[-1].split('.')[-1]
        filename_save = f"{l['name'].replace(' ', '_')}.{filetype}"

        #สร้าง floder data สำหรับจัดเก็บไฟล์
        directory_name = "data"
        #สร้าง directory
        try:
            os.mkdir(directory_name)
            print(f"Directory '{directory_name}' created successfully.")
        except FileExistsError:
            print(f"Directory '{directory_name}' already exists.")
        except PermissionError:
            print(f"Permission denied: Unable to create '{directory_name}'.")
        except Exception as e:
            print(f"An error occurred: {e}")

        response_file = requests.request("GET", l['url']) #ไปเอาลิส package ทั้งหมด
        if response_file.status_code == 200:
            with open(f"./data/{filename_save}", "wb") as f:
                f.write(response_file.content)
            print(f"ดาวน์โหลดไฟล์สำเร็จ: {filename_save}")
        else:
            print(f"ไม่สามารถดาวน์โหลดไฟล์ (status code: {response_file.status_code})")
        # print(f"{package_name} - {l['name']} - {l['id']} - {l['format']}")
    

