# from airflow.decorators import dag, task
# from airflow.models import Variable
# from datetime import datetime
# from minio import Minio
import requests

# # API_KEY = ""
# BUCKET_NAME = "dataset-covid19-cases"
# MINIO_ENDPOINT = "minio:9000"
# MINIO_ACCESS_KEY = "minio"
# MINIO_SECRET_KEY = "minio123"

# @dag(
#     dag_id="fetch_covid19_cases_pipeline", 
#     schedule=None, 
#     start_date=datetime(2023, 8, 1), 
#     catchup=False
# )
# def fetch_covid19_cases_pipeline():
#     @task
def fetch_data():
        url = "https://opend.data.go.th/get-ckan/package_search?q="
        headers = {
            "api-key" : "Np76FNlQTpjMNoUPFJkJc0Nf7cv63vhd"
        }

        package_name = "covid-19-daily"
        all_resource = requests.get(url + package_name, headers=headers)

        # #upload to minio
        # minio_clienct = Minio(
        #     MINIO_ENDPOINT,
        #     access_key=MINIO_ACCESS_KEY,
        #     secret_key=MINIO_SECRET_KEY,
        #     secure=False
        # )

        # if not minio_clienct.bucket_exists(BUCKET_NAME):
        #     minio_clienct.make_bucket(BUCKET_NAME)

        for i in range(all_resource['result']):
            # name = all_resource['result'][i]['name']
            resource = requests.get(all_resource['result'][i]['url'], headers=headers)
            filetype = i['url'].split('/')[-1].split('.')[-1]
            filename_save = f"{i['name'].replace(' ', '_')}.{filetype}"
     


        # return resource
#         filename_save = name + ".json"
#         if resource.status_code == 200:
#                 with open(f"./data/{filename_save}", "wb") as f:
#                     f.write(resource.content)   
# if __name__ == "__main__":
#     fetch_data()

        


#     @task
#     def upload_to_minio():
#         pass
#     @task
#     def minio_to_postgres():
#         pass

#     fetch_data()
#     upload_to_minio()
#     minio_to_postgres()
# dag = fetch_covid19_cases_pipeline()
