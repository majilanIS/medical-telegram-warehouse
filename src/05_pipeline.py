from dagster import job, op
import subprocess

# Step 1: Scrape Telegram data
@op
def scrape_telegram_data():
    subprocess.run(["python", "src/01_scraper.py"], check=True)

# Step 2: Load JSON to PostgreSQL
@op
def load_raw_to_postgres():
    subprocess.run(["python", "src/02_Load_RawData.py"], check=True)

# Step 3: Run dbt transformations
@op
def run_dbt_transformations():
    subprocess.run(["dbt", "run", "--project-dir", "."], check=True)

# Step 4: Run YOLO enrichment
@op
def run_yolo_enrichment():
    subprocess.run(["python", "src/03_yolo_detect_create_CSV.py"], check=True)
    subprocess.run(["python", "src/03_yolo_load_to_postgres.py"], check=True)

# Job that links all ops
@job
def telegram_data_pipeline():
    scrape = scrape_telegram_data()
    load = load_raw_to_postgres()
    dbt = run_dbt_transformations()
    yolo = run_yolo_enrichment()

    # Define execution order
    load.after(scrape)
    dbt.after(load)
    yolo.after(dbt)
