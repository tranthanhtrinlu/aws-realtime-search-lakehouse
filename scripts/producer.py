import json
import time

import pandas as pd
from kafka import KafkaProducer

# ==== CHỈNH 4 DÒNG NÀY CHO ĐÚNG MÁY BẠN ====
PARQUET_FILE = r"E:\DataSetDataEngineer\log_search\20220601\part-00000-a4016f8f-c573-42d0-aa92-3367f02f0c94-c000.snappy.parquet"
TARGET_DATE = "2022-06-01"   # ngày thật của file này, lấy từ tên folder chứa nó
DELAY_SECONDS = 0.001  # giảm xuống để gửi nhanh, test quy mô lớn (không cần giống thật nữa)
LIMIT = None           # None = gửi hết cả ngày (~82K dòng), không giới hạn nữa
# =============================================

TOPIC = "search-events"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(f"Đang đọc file: {PARQUET_FILE}")
df = pd.read_parquet(PARQUET_FILE)

# Loại các dòng có datetime lỗi định dạng, rồi lọc theo khoảng gần ngày thật của file
# (thiết bị có thể lệch đồng hồ sai năm, sai tháng, hoặc dùng lịch Phật giáo Thái Lan —
#  ở đây lọc thô cho demo bằng cách bám vào ngày thật (TARGET_DATE) thay vì tin theo cột datetime;
#  xử lý triệt để hơn, như quy đổi lịch Phật giáo, sẽ làm ở tầng Silver)
df["datetime_parsed"] = pd.to_datetime(df["datetime"], errors="coerce")
before = len(df)
df = df.dropna(subset=["datetime_parsed"])
target = pd.Timestamp(TARGET_DATE)
df = df[(df["datetime_parsed"] >= target - pd.Timedelta(days=2)) & (df["datetime_parsed"] <= target + pd.Timedelta(days=2))]
df = df.sort_values("datetime_parsed")
print(f"Loại {before - len(df)} dòng datetime lỗi/lệch quá xa ngày {TARGET_DATE}, còn lại {len(df)} dòng hợp lệ.")

if LIMIT:
    df = df.head(LIMIT)

print(f"Chuẩn bị gửi {len(df)} event vào topic '{TOPIC}'...\n")

sent = 0
for _, row in df.iterrows():
    event = json.loads(row.drop("datetime_parsed").to_json())
    producer.send(TOPIC, value=event)
    sent += 1
    if sent % 100 == 0:
        print(f"Đã gửi {sent}/{len(df)} event...")
    time.sleep(DELAY_SECONDS)

producer.flush()
print(f"\nXong! Đã gửi tổng cộng {sent} event vào Kafka.")