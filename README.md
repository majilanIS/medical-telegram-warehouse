# Medical Telegram Data Warehouse

## Overview

This project builds a **data pipeline** to extract, load, and transform messages from Telegram channels related to Ethiopian medical businesses.  
It consists of **Task-1 (Data Scraping)** and **Task-2 (Data Modeling & Transformation)**.

---

## Task 1: Data Scraping and Collection

**Objective:** Extract messages and images from Telegram channels and store raw data in a structured data lake.

**Steps:**

1. **Telegram API Access**: Registered at `my.telegram.org` and obtained API credentials.
2. **Scraper**: Built using Python + Telethon.
3. **Extracted Data**:
   - Message ID, date, text content
   - View count, forward count
   - Media information (photos)
4. **Downloaded Images**: Stored in folder structure:  
   `data/raw/images/{channel_name}/{message_id}.jpg`
5. **Raw Data Lake**: JSON files stored by date & channel:  
   `data/raw/telegram_messages/YYYY-MM-DD/{channel_name}.json`
6. **Logging**: Logs of scraping activity stored in `logs/`.

**Deliverables**:

- `src/scraper.py` — working scraper script
- JSON files in `data/raw/telegram_messages/`
- Downloaded images
- Logs in `logs/`

---

## Task 2: Data Modeling and Transformation

**Objective:** Transform raw Telegram data into a **structured data warehouse** using PostgreSQL and dbt.

**Steps:**

1. **Load Raw Data to PostgreSQL**:
   - Python script (`src/02_Load_RawData.py`) reads JSON files and inserts into table:  
     `raw.telegram_messages`
   - Handles all messages, emojis, and special characters.
2. **Raw Table Schema**:

| Column       | Type      |
| ------------ | --------- |
| message_id   | TEXT PK   |
| channel_name | TEXT      |
| message_date | TIMESTAMP |
| message_text | TEXT      |
| has_media    | BOOLEAN   |
| image_path   | TEXT      |
| views        | INTEGER   |
| forwards     | INTEGER   |

3. **dbt Models (Next Step)**:
   - Staging models clean and standardize raw data.
   - Mart models implement star schema:
     - **Dimensions**: `dim_channels`, `dim_dates`
     - **Fact table**: `fct_messages`

**Deliverables**:

- Raw table `raw.telegram_messages` populated in PostgreSQL
- Python ETL script: `src/02_Load_RawData.py`
- Ready for dbt staging and marts

---

## Notes

- **Encoding**: PostgreSQL stores text in UTF-8; Windows console may need `chcp 65001` to display emojis.
- **Data Count**: Currently 8046 messages loaded from Telegram channels.
- **Next Steps**: Task-3 (YOLO object detection) to enrich images and integrate with the warehouse.
