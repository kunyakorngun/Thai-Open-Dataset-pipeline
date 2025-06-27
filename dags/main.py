from airflow.decorators import dag, task
from datetime import datetime
from minio import Minio
from io import BytesIO
import requests
import os
import json
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# --- CONFIG ---
API_KEY = "Np76FNlQTpjMNoUPFJkJc0Nf7cv63vhd"
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "covid-19-daily"
PACKAGE_NAME = "covid-19-daily"
LOG_PATH = "/opt/airflow/logs/resource_log.log"

# PostgreSQL config
PG_HOST = "postgres_db"
PG_PORT = 5432
PG_DB = "postgres"
PG_USER = "postgres"
PG_PASSWORD = "postgres123"

@dag(
    dag_id="covid19_pipeline",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
)
def pipeline():

    # อ่านไฟล์ log ที่เป็น JSON Lines และเก็บข้อมูลล่าสุดของ resource ว่าโหลดมารึยัง
    def load_existing_logs():
        logs = {}
        if os.path.exists(LOG_PATH):
            with open(LOG_PATH, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        entry = json.loads(line)
                        key = (entry["package_id"], entry["resource_id"])
                        logs[key] = entry["last_modified"]
                    except Exception:
                        continue
        return logs

    # บันทึก log ใหม่ ทั้งลงไฟล์และ PostgreSQL
    def append_log_entry(package_id, resource_id, last_modified):
        log_entry = {
            "package_id": package_id,
            "resource_id": resource_id,
            "last_modified": last_modified,
            "timestamp": datetime.utcnow().isoformat()
        }
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT,
                database=PG_DB, user=PG_USER, password=PG_PASSWORD
            )
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS resource_logs (
                    id SERIAL PRIMARY KEY,
                    package_id TEXT,
                    resource_id TEXT,
                    last_modified TEXT,
                    timestamp TIMESTAMP
                )
            """)
            cursor.execute("""
                INSERT INTO resource_logs (package_id, resource_id, last_modified, timestamp)
                VALUES (%s, %s, %s, %s)
            """, (package_id, resource_id, last_modified, datetime.utcnow()))
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Error writing log to PostgreSQL: {e}")

    # เพิ่มข้อมูลลงตาราง resource_mapping
    def insert_resource_mapping(table_name, filename, url):
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT,
                database=PG_DB, user=PG_USER, password=PG_PASSWORD
            )
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS resource_mapping (
                    id SERIAL PRIMARY KEY,
                    table_name TEXT,
                    original_filename TEXT,
                    url TEXT,
                    timestamp TIMESTAMP
                )
            """)
            cursor.execute("""
                INSERT INTO resource_mapping (table_name, original_filename, url, timestamp)
                VALUES (%s, %s, %s, %s)
            """, (table_name, filename, url, datetime.utcnow()))
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Error inserting resource mapping: {e}")

    # ดึง package จาก CKAN API
    @task()
    def get_package_detail():
        headers = {"api-key": API_KEY}
        url = f"https://data.go.th/api/3/action/package_show?id={PACKAGE_NAME}"
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code != 200:
            raise ValueError(f"Failed to fetch package: {PACKAGE_NAME}")
        pkg = res.json()["result"]
        return {
            "package_id": PACKAGE_NAME,
            "title": pkg.get("title", PACKAGE_NAME),
            "resources": pkg.get("resources", [])
        }

    # อัปโหลด resource ใหม่ขึ้น MinIO พร้อมบันทึก log และ mapping
    @task()
    def upload_to_minio(pkg_data: dict):
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        if not minio_client.bucket_exists(BUCKET_NAME):
            minio_client.make_bucket(BUCKET_NAME)

        existing_logs = load_existing_logs()
        title = pkg_data["title"].replace("/", "_").replace("\\", "_").strip()

        uploaded_resources = []
        for resource in pkg_data["resources"]:
            resource_id = resource["id"]
            last_modified = resource.get("last_modified", "")
            key = (pkg_data["package_id"], resource_id)

            if key in existing_logs and existing_logs[key] == last_modified:
                print(f"Skipping already uploaded resource: {key}")
                continue

            download_url = resource.get("original_url") or resource.get("url")
            if not download_url:
                print(f"No download URL for resource: {resource_id}")
                continue

            try:
                r = requests.get(download_url, timeout=20)
                if r.status_code != 200:
                    print(f"Failed to download resource: {download_url}")
                    continue

                file_data = r.content
                name = resource.get("name", resource_id).strip().replace("/", "_")
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
                print(f"Uploaded to MinIO: {object_path}")

                insert_resource_mapping(table_name=f"table_{resource_id[:8]}", filename=filename, url=download_url)
                append_log_entry(pkg_data["package_id"], resource_id, last_modified)

                uploaded_resources.append(resource)

            except Exception as e:
                print(f"Error uploading {download_url} - {e}")

        return {"package_id": pkg_data["package_id"], "title": title, "resources": uploaded_resources}

    # โหลดไฟล์จาก MinIO แล้วนำเข้า PostgreSQL
    @task()
    def load_to_postgres(pkg_data: dict):
        from sqlalchemy import create_engine

        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

        title = pkg_data["title"].replace("/", "_").replace("\\", "_").strip()

        for resource in pkg_data["resources"]:
            resource_id = resource["id"]
            name = resource.get("name", resource_id).strip().replace("/", "_")
            ext = os.path.splitext(resource.get("url", ""))[-1] or ".csv"
            filename = f"{name}{ext}"
            object_path = f"{title}/{filename}"

            try:
                obj = minio_client.get_object(BUCKET_NAME, object_path)
                content = obj.read()

                try:
                    df = pd.read_csv(BytesIO(content), encoding="utf-8", low_memory=False)
                except UnicodeDecodeError:
                    df = pd.read_csv(BytesIO(content), encoding="cp874", low_memory=False)

                # Clean and normalize column names
                df.columns = [
                    col.strip().lower().replace(" ", "_").replace(".", "").replace("-", "_")
                    for col in df.columns
                ]

                # Define dtype map
                dtype_map = {
                    "no": "float64",
                    "age": "Int64",
                    "sex": "str",
                    "nationality": "str",
                    "province_of_isolation": "str",
                    "province_of_onset": "str",
                    "district_of_onset": "str",
                    "quarantine": "str",
                }
                date_cols = ["notified_date", "announce_date"]

                # Cast dtypes
                for col, dtype in dtype_map.items():
                    if col in df.columns:
                        if dtype == "Int64":
                            # Only accept values without decimal and coercible to int
                            df[col] = pd.to_numeric(df[col], errors="coerce")
                            df[col] = df[col].apply(lambda x: int(x) if pd.notna(x) and x == int(x) else pd.NA).astype("Int64")
                        elif dtype == "float64":
                            df[col] = pd.to_numeric(df[col], errors="coerce")
                        else:
                            df[col] = df[col].astype(str)

                for col in date_cols:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

                # Drop rows with NaN in critical fields only
                required_cols = ["no", "age", "sex", "nationality", "province_of_isolation"]
                df.dropna(subset=[col for col in required_cols if col in df.columns], inplace=True)

                # Prepare table
                table_name = f"table_{resource_id[:8]}"

                pg_type_map = {
                    "int64": "INTEGER",
                    "float64": "FLOAT",
                    "object": "TEXT",
                    "bool": "BOOLEAN",
                    "datetime64[ns]": "TIMESTAMP",
                    "Int64": "INTEGER"
                }

                columns_sql = []
                for col in df.columns:
                    dtype = str(df[col].dtype)
                    pg_type = pg_type_map.get(dtype, "TEXT")
                    columns_sql.append(f'"{col}" {pg_type}')

                conn = psycopg2.connect(
                    host=PG_HOST,
                    port=PG_PORT,
                    database=PG_DB,
                    user=PG_USER,
                    password=PG_PASSWORD
                )
                cursor = conn.cursor()
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_sql)});")
                conn.commit()

                # Insert with psycopg2
                for _, row in df.iterrows():
                    values = tuple(None if pd.isna(v) else v for v in row)
                    placeholders = ", ".join(["%s"] * len(values))
                    cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", values)

                conn.commit()
                cursor.close()
                conn.close()
                print(f"✅ Loaded to PostgreSQL: {table_name}")

            except Exception as e:
                print(f"❌ Error loading resource {resource_id} to postgres: {e}")
            
 

    @task()
    def create_data_marts():
        # Connection config
        print(" Connecting to PostgreSQL databases...")

        mart_conn = psycopg2.connect( #แก้ให้มันเก็บใน database postgres อันเก่า
            host="postgres_db",
            port=5432,
            database="data_mart",
            user="postgres",
            password="postgres123"
        )

        main_conn = psycopg2.connect(
            host="postgres_db",
            port=5432,
            database="postgres",
            user="postgres",
            password="postgres123"
        )

        print("Connected to all databases.")

        #ดึงรายชื่อ table ที่ต้องใช้
        main_cursor = main_conn.cursor()
        main_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        table_names = [r[0] for r in main_cursor.fetchall() if r[0].startswith("table_")]
        print(f"Found tables: {table_names}")

        #เตรียม dataframe เปล่าสำหรับเก็บข้อมูล
        mart_df = pd.DataFrame()
        #อ่านข้อมูล chunk ละ 1000 แถว
        for table in table_names:
            try:
                print(f" Reading data from table: {table}")
                chunks = pd.read_sql_query(f'SELECT * FROM "{table}"', main_conn, chunksize=1000)
                #ตรวจสอบว่ามี column ที่ต้องใช้มั้ย
                for chunk in chunks:
                    mart_cols = ["province_of_isolation", "province_of_onset", "district_of_onset"]
                    mart_cols = [col for col in mart_cols if col in chunk.columns]
                    if len(mart_cols) == 3:
                        print(f" Adding data to mart from table: {table}")
                        #ถ้ามีก็รวมใส่ mart_df
                        mart_df = pd.concat([mart_df, chunk[mart_cols]], ignore_index=True)

            except Exception as e:
                print(f" Error reading table {table}: {e}")

        # Load to mart
        print(" Loading data into mart...")
        mart_cursor = mart_conn.cursor()
        mart_cursor.execute("DROP TABLE IF EXISTS data_mart") #ลบตารางเดิมออกถ้ามีแล้ว
        #สรา้งตารางใหม่
        mart_cursor.execute(""" 
            CREATE TABLE data_mart (
                province_of_isolation TEXT,
                province_of_onset TEXT,
                district_of_onset TEXT
            )
        """)
        #loop ใส่ข้อมูลทีละแถว
        for _, row in mart_df.iterrows():
            mart_cursor.execute(
                "INSERT INTO data_mart (province_of_isolation, province_of_onset, district_of_onset) VALUES (%s, %s, %s)",
                tuple(None if pd.isna(v) else v for v in row)
            )
        mart_conn.commit()
        print(" Finished loading data_mart")

        mart_cursor.close()
        main_cursor.close()
        mart_conn.close()
        main_conn.close()

        print(" Data marts created successfully.")

    # === DAG flow ===
    pkg = get_package_detail()
    uploaded_pkg = upload_to_minio(pkg)
    # load_to_postgres(uploaded_pkg)
    load_to_postgres(uploaded_pkg) >> create_data_marts()

dag = pipeline()