import boto3
import os
import re

SOURCE_DIR = r"E:\DataSetDataEngineer\log_search"
BUCKET_NAME = "thanhtri-logsearch-lakehouse-dev"
# ============================================

s3 = boto3.client('s3', region_name='ap-southeast-1')

total_files = 0
for folder_name in sorted(os.listdir(SOURCE_DIR)):
    folder_path = os.path.join(SOURCE_DIR, folder_name)
    if not os.path.isdir(folder_path):
        continue

    match = re.match(r'^(\d{4})(\d{2})(\d{2})$', folder_name)
    if not match:
        print(f"[BỎ QUA] folder không đúng định dạng ngày: {folder_name}")
        continue

    year, month, day = match.groups()
    date_partition = f"date={year}-{month}-{day}"

    for file_name in os.listdir(folder_path):
        if not file_name.endswith('.parquet'):
            continue
        local_file = os.path.join(folder_path, file_name)
        s3_key = f"raw/logsearch/{date_partition}/{file_name}"
        try:
            s3.upload_file(local_file, BUCKET_NAME, s3_key)
            print(f"[OK] {folder_name}/{file_name} -> s3://{BUCKET_NAME}/{s3_key}")
            total_files += 1
        except Exception as e:
            print(f"[LỖI] {folder_name}/{file_name}: {e}")

print(f"\nXong! Đã upload {total_files} file lên S3.")