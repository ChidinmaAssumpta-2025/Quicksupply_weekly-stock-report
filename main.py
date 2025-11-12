import pandas as pd
import requests
import io
import psycopg2
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os

# =========================================================
# 1️⃣ Load environment variables
# =========================================================
print("🔍 Loading environment variables...")
load_dotenv()

# Kobo credentials
KOBO_USERNAME = os.getenv("KOBO_USERNAME")
KOBO_PASSWORD = os.getenv("KOBO_PASSWORD")
KOBO_CSV_URL = os.getenv("KOBO_URL")

# PostgreSQL credentials
PG_HOST = os.getenv("PG_HOST")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")

# Schema and table details
schema_name = "anne_2"
table_name = "tinto"

# =========================================================
# 2️⃣ Fetch data from KoboToolbox
# =========================================================
print("📡 Fetching data from KoboToolbox...")
response = requests.get(KOBO_CSV_URL, auth=HTTPBasicAuth(KOBO_USERNAME, KOBO_PASSWORD))

if response.status_code == 200:
    print("✅ Data fetched successfully!\n")

    csv_data = io.StringIO(response.text)
    df = pd.read_csv(csv_data, sep=',', on_bad_lines='skip')

    # =========================================================
    # 3️⃣ Clean and transform data
    # =========================================================
    print("🧹 Cleaning and transforming data...")

    # Standardize column names
    df.columns = [col.strip().replace(" ", "_").replace("-", "_").replace("&", "and").lower() for col in df.columns]

    # Convert date fields to datetime format (if applicable)
    if "date_of_reporting" in df.columns:
        df["date_of_reporting"] = pd.to_datetime(df["date_of_reporting"], errors='coerce')

    print(f"📊 Data contains {len(df)} rows and {len(df.columns)} columns.")
    print("✅ Data cleaned successfully!\n")

    # =========================================================
    # 4️⃣ Connect to PostgreSQL
    # =========================================================
    print("🧩 Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT
    )
    cur = conn.cursor()
    print("✅ Connected to PostgreSQL!\n")

    # =========================================================
    # 5️⃣ Create schema and table
    # =========================================================
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

    # Drop & recreate table (optional — fresh load)
    cur.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name};")

    create_table_query = f"""
        CREATE TABLE {schema_name}.{table_name} (
            id SERIAL PRIMARY KEY,
            start TIMESTAMPTZ,
            "end" TIMESTAMPTZ,
            branch_location TEXT,
            date_of_reporting DATE,
            select_the_product_name TEXT,
            select_product_category TEXT,
            opening_stock_for_week INT,
            quantity_received_this_week INT,
            units_sold_this_week INT,
            delay_in_supply_this_week TEXT,
            delay_days INT,
            recommendation TEXT,
            _id INT,
            _uuid TEXT,
            _submission_time TIMESTAMPTZ,
            _validation_status TEXT,
            _notes TEXT,
            _status TEXT,
            _submitted_by TEXT,
            __version__ TEXT,
            _tags TEXT,
            _index INT
        );
    """
    cur.execute(create_table_query)
    print(f"✅ Table '{schema_name}.{table_name}' created successfully!\n")

    # =========================================================
    # 6️⃣ Insert data into PostgreSQL
    # =========================================================
    print("📥 Inserting data into PostgreSQL...")

    insert_query = f"""
        INSERT INTO {schema_name}.{table_name} (
            start, "end", branch_location, date_of_reporting,
            select_the_product_name, select_product_category,
            opening_stock_for_week, quantity_received_this_week,
            units_sold_this_week, delay_in_supply_this_week, delay_days,
            recommendation, _id, _uuid, _submission_time,
            _validation_status, _notes, _status, _submitted_by,
            __version__, _tags, _index
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
    """

    for _, row in df.iterrows():
        cur.execute(insert_query, (
            row.get("start"),
            row.get("end"),
            row.get("branch_location"),
            row.get("date_of_reporting"),
            row.get("select_the_product_name"),
            row.get("select_product_category"),
            int(row.get("what_was_the_opening_stock_for_the_week", 0)) if pd.notna(row.get("what_was_the_opening_stock_for_the_week")) else 0,
            int(row.get("what_was_the_quantity_of_the_product_recieved_this_week?", 0)) if pd.notna(row.get("what_was_the_quantity_of_the_product_recieved_this_week?")) else 0,
            int(row.get("how_many_units_of_this_product_were_sold_this_week?", 0)) if pd.notna(row.get("how_many_units_of_this_product_were_sold_this_week?")) else 0,
            row.get("was_there_any_delay_in_supply_this_week?"),
            int(row.get("if_yes,_how_many_days_was_the_delay?", 0)) if pd.notna(row.get("if_yes,_how_many_days_was_the_delay?")) else 0,
            row.get("what_recommendation_would_you_like_to_make_to_improve_product_availability_or_supply?"),
            row.get("_id"),
            row.get("_uuid"),
            row.get("_submission_time"),
            row.get("_validation_status"),
            row.get("_notes"),
            row.get("_status"),
            row.get("_submitted_by"),
            row.get("__version__"),
            row.get("_tags"),
            row.get("_index")
        ))

    conn.commit()
    print(f"✅ {len(df)} records inserted successfully into '{schema_name}.{table_name}'!\n")

    # =========================================================
    # 7️⃣ Close connection
    # =========================================================
    cur.close()
    conn.close()
    print("🔒 Connection closed. ETL pipeline completed successfully! 🚀")

else:
    print(f"❌ Failed to fetch data. Status code: {response.status_code}")
