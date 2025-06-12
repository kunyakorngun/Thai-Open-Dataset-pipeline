from minio import Minio 
import pandas as pd
import psycopg2
import io
import re
import chardet

# ----------------------
# 1. ตั้งค่า MinIO
# ----------------------
minio_client = Minio(
    endpoint="localhost:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False
)

bucket_name = "dataset-bucket"

# ----------------------
# 2. ตั้งค่า PostgreSQL
# ----------------------
pg_conn = psycopg2.connect(
    host="localhost",
    port=5431,
    dbname="postgres",
    user="postgres",
    password="postgres123"
)
pg_cursor = pg_conn.cursor()

# ----------------------
# 3. ฟังก์ชันช่วยให้ชื่อ table ภาษาไทยปลอดภัย
# ----------------------
def safe_table_name(filename):
    name = re.sub(r'\.csv$', '', filename.split("/")[-1])
    name = name.strip().replace(" ", "_")
    name = re.sub(r'[^\w\u0E00-\u0E7F]', '_', name)  # อนุญาตภาษาไทย
    return name

# ----------------------
# 4. อ่านไฟล์จาก MinIO และโหลดเข้า PostgreSQL
# ----------------------
objects = minio_client.list_objects(bucket_name, recursive=True)

for obj in objects:
    if not obj.object_name.endswith(".csv"):
        continue

    table_name = safe_table_name(obj.object_name)

    response = minio_client.get_object(bucket_name, obj.object_name)
    csv_data = response.read()

    # ตรวจ encoding ด้วย chardet
    detected_encoding = chardet.detect(csv_data[:10000])['encoding']
    print(f"📌 Detected encoding for {obj.object_name}: {detected_encoding}")

    try:
        # ตรวจว่า decode ด้วย UTF-8 ได้หรือไม่
        _ = csv_data.decode("utf-8-sig")
        print(f"✅ {obj.object_name} is valid UTF-8 (with or without BOM)")
        df = pd.read_csv(io.BytesIO(csv_data), encoding="utf-8-sig")
    except UnicodeDecodeError:
        print(f"❌ {obj.object_name} is NOT UTF-8. ใช้ encoding ที่ตรวจพบ: {detected_encoding}")
        try:
            df = pd.read_csv(io.BytesIO(csv_data), encoding=detected_encoding)
        except Exception as e:
            print(f"⚠️ อ่านด้วย {detected_encoding} ไม่ได้: {e}")
            print("➡️ ลอง fallback ไป ISO-8859-11 (TIS-620)")
            df = pd.read_csv(io.BytesIO(csv_data), encoding="ISO-8859-11")

    if df.empty:
        print(f"⚠️ ไม่มีข้อมูลในไฟล์: {obj.object_name}")
        continue

    print(f"🔍 Preview of {obj.object_name}:")
    print(df.head(3))

    # สร้างตารางอัตโนมัติ
    col_defs = ", ".join([f'"{col}" TEXT' for col in df.columns])
    create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs});'
    pg_cursor.execute(create_sql)
    pg_conn.commit()

    # insert ข้อมูล
    for _, row in df.iterrows():
        # แปลงค่าทั้งหมดเป็น str และจัดการ NaN → None
        values = tuple(str(v).strip() if pd.notna(v) else None for v in row.values)
        placeholders = ', '.join(['%s'] * len(values))
        insert_sql = f'INSERT INTO "{table_name}" VALUES ({placeholders});'

        try:
            pg_cursor.execute(insert_sql, values)
        except Exception as e:
            print(f"❌ Insert failed for row: {values}")
            print(f"💥 Error: {e}")

    pg_conn.commit()
    print(f"✅ Loaded: {obj.object_name} → table: {table_name}")

# ----------------------
# 5. ปิดการเชื่อมต่อ
# ----------------------
pg_cursor.close()
pg_conn.close()
