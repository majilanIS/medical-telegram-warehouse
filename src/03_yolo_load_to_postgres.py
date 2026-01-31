import psycopg2
import csv

# PostgreSQL connection
conn = psycopg2.connect(
    user="postgres",
    password="postgres@2025",
    host="localhost",
    port="5432",
    database="medical_telegram_db"
)
cursor = conn.cursor()

# Path to CSV created by first script
CSV_PATH = "../data/yolo_results.csv"

# Open CSV and insert rows
with open(CSV_PATH, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        cursor.execute("""
            INSERT INTO raw.yolo_detections (message_id, channel_name, detected_objects, confidence_score, image_category)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            row["message_id"],
            row["channel_name"],
            row["detected_objects"],
            row["confidence_score"],
            row["image_category"]
        ))

conn.commit()
cursor.close()
conn.close()

print("YOLO results successfully loaded into PostgreSQL!")