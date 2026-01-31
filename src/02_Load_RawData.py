import json
import psycopg2
import glob
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

conn = psycopg2.connect(
    user="postgres",
    password="postgres@2025",
    host="localhost",
    port="5432",
    database="medical_telegram_db"
)

cursor = conn.cursor()

files = glob.glob(
    os.path.join(BASE_DIR, "../data/raw/telegram_messages/**/*.json"),
    recursive=True
)

print(f"Found {len(files)} files")

for file in files:
    with open(file, "r", encoding="utf-8") as f:
        messages = json.load(f)

    for m in messages:
        try:
            cursor.execute("""
                INSERT INTO raw.telegram_messages (
                    message_id,
                    channel_name,
                    message_date,
                    message_text,
                    has_media,
                    image_path,
                    views,
                    forwards
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (message_id) DO NOTHING
            """, (
                m.get("message_id"),
                m.get("channel_name"),
                m.get("message_date"),
                m.get("message_text"),
                m.get("has_media"),
                m.get("image_path"),
                m.get("views"),
                m.get("forwards")
            ))

        except Exception as e:
            print(f"Error inserting message {m.get('message_id')}: {e}")
            conn.rollback()

conn.commit()
cursor.close()
conn.close()

print("Load complete.")
