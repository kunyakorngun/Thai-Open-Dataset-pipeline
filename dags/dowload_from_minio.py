from minio import Minio 
import pandas as pd
import psycopg2
import io
import re
import chardet

# ✅ ตั้งค่าการเชื่อมต่อ MinIO
minio_client = Minio(
    endpoint="localhost:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False
)

bucket_name = "dataset-bucket"

# ✅ ตั้งค่าการเชื่อมต่อ PostgreSQL
pg_conn = psycopg2.connect(
    host="localhost",
    port=5431,
    dbname="postgres",
    user="postgres",
    password="postgres123"
)
pg_cursor = pg_conn.cursor()

# ✅ สร้างตารางสำหรับ mapping ถ้ายังไม่มี
pg_cursor.execute("""
CREATE TABLE IF NOT EXISTS dataset_table_mapping (
    id SERIAL PRIMARY KEY,
    table_name TEXT,
    original_filename TEXT
);
""")
pg_conn.commit()

# ✅ เริ่มนับชื่อ table จาก 1
table_counter = 1

# ✅ วนลูปอ่านไฟล์ใน bucket
objects = minio_client.list_objects(bucket_name, recursive=True)
for obj in objects:
    if not obj.object_name.endswith(".csv"):
        continue

    original_filename = obj.object_name.split("/")[-1]
    table_name = f"tbl_{table_counter:03d}"
    table_counter += 1

    # ✅ ดาวน์โหลดไฟล์จาก MinIO
    response = minio_client.get_object(bucket_name, obj.object_name)
    csv_data = response.read()

    # ✅ ตรวจ encoding
    detected_encoding = chardet.detect(csv_data[:10000])['encoding']
    print(f"📌 Detected encoding for {original_filename}: {detected_encoding}")

    # ✅ แปลงเป็น DataFrame
    try:
        _ = csv_data.decode("utf-8-sig")
        df = pd.read_csv(io.BytesIO(csv_data), encoding="utf-8-sig")
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(io.BytesIO(csv_data), encoding=detected_encoding)
        except Exception as e:
            print(f"⚠️ อ่านด้วย {detected_encoding} ไม่ได้: {e}")
            df = pd.read_csv(io.BytesIO(csv_data), encoding="ISO-8859-11")

    if df.empty:
        print(f"⚠️ ไม่มีข้อมูลในไฟล์: {original_filename}")
        continue

    print(f"🔍 Preview of {original_filename}:")
    print(df.head(3))

    # ✅ สร้างตารางใน Postgres ตาม column
    col_defs = ", ".join([f'"{col}" TEXT' for col in df.columns])
    create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs});'
    pg_cursor.execute(create_sql)
    pg_conn.commit()

    # ✅ insert ข้อมูลทีละแถว
    for _, row in df.iterrows():
        values = tuple(str(v).strip() if pd.notna(v) else None for v in row.values)
        placeholders = ', '.join(['%s'] * len(values))
        insert_sql = f'INSERT INTO "{table_name}" VALUES ({placeholders});'
        try:
            pg_cursor.execute(insert_sql, values)
        except Exception as e:
            print(f"Insert failed for row: {values}")
            print(f"Error: {e}")

    # ✅ บันทึกความสัมพันธ์ลงใน mapping table
    pg_cursor.execute("""
        INSERT INTO dataset_table_mapping (table_name, original_filename)
        VALUES (%s, %s);
    """, (table_name, original_filename))

    pg_conn.commit()
    print(f"✅ Loaded: {original_filename} → table: {table_name}")

# ✅ ปิดการเชื่อมต่อ
pg_cursor.close()
pg_conn.close()
