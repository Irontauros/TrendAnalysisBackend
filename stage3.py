import mysql.connector
import threading
import time
import requests
import os
from decimal import Decimal

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "mistral"

# --- DB Connection ---
def get_connection(db_config):
    return mysql.connector.connect(**db_config)

# --- Reset future table ---
def clear_future_table(db_config):
    conn = get_connection(db_config)
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE future")
    conn.commit()
    cursor.close()
    conn.close()
    print("🧹 Cleared and reset 'future' table.", flush=True)

# --- Fetch totals ---
def fetch_historical_data(db_config):
    conn = get_connection(db_config)
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT DISTINCT year FROM totals ORDER BY year")
    all_years = [row['year'] for row in cursor.fetchall()]
    last_year = max(all_years)

    cursor.execute("SELECT DISTINCT category FROM totals ORDER BY category")
    all_categories = [row['category'] for row in cursor.fetchall()]

    data_by_category = {}
    for category in all_categories:
        yearly_totals = {year: 0 for year in all_years}

        cursor.execute("""
            SELECT year, SUM(article_count) AS total
            FROM totals
            WHERE category = %s
            GROUP BY year
        """, (category,))
        rows = cursor.fetchall()

        for row in rows:
            yearly_totals[row['year']] = int(row['total']) if isinstance(row['total'], Decimal) else row['total']

        data_by_category[category] = {
            "history": [{"year": year, "total": yearly_totals[year]} for year in all_years],
            "last_year": last_year
        }

    cursor.close()
    conn.close()
    return data_by_category

# --- Save to DB ---
def save_predictions(db_config, category, predictions):
    conn = get_connection(db_config)
    cursor = conn.cursor()
    for entry in predictions:
        year = entry.get("year")
        total = entry.get("predicted_total")
        if year and total is not None:
            cursor.execute(
                "INSERT INTO future (category, year, predicted_total) VALUES (%s, %s, %s)",
                (category, year, int(float(total)))
            )
    conn.commit()
    cursor.close()
    conn.close()

# --- Prompt Builder ---
def build_prompt(history, target_years):
    return f"""
You are a trend-based prediction engine. Based only on the input data below, use linear trend extrapolation to predict article counts for the following years: {target_years}.

Guidelines:
- Use the trend of the historical data to make consistent, linear predictions.
- The values should reflect expected increases or decreases based on past data.
- Do not use random guesses. Base your predictions on the rate of change in the input data.
- All predicted totals must be non-negative integers.
- Return only a JSON array — no explanations or formatting.
- Include exactly one item for each of these years: {target_years}

Return format:
[
  {{ "year": YEAR, "predicted_total": NUMBER }},
  ...
]

Historical input data:
{history}
""".strip()

# --- Predict via AI ---
def predict_future(category, history, target_years):
    print(f"\n🔮 Category: {category}", flush=True)
    print("📘 Historical Data:", flush=True)
    for h in history:
        print(f"  {h['year']}: {h['total']}", flush=True)

    for attempt in range(3):
        try:
            prompt = build_prompt(history, target_years)
            payload = {
                "model": MODEL,
                "prompt": prompt,
                "stream": False,
                "temperature": 0
            }

            response = requests.post(OLLAMA_URL, json=payload)
            response.raise_for_status()
            raw_output = response.json().get("response", "").strip()

            json_start = raw_output.find("[")
            json_end = raw_output.rfind("]") + 1
            predictions = eval(raw_output[json_start:json_end])  # Safe-ish since you control the prompt

            predicted_years = {p["year"] for p in predictions}
            if set(target_years) != predicted_years:
                raise ValueError(f"Incomplete prediction: expected {target_years}, got {predicted_years}")

            print("🤖 AI Prediction:", flush=True)
            for pred in predictions:
                print(f"  {pred['year']}: {pred['predicted_total']}", flush=True)

            return predictions

        except Exception as e:
            print(f"❌ Attempt {attempt+1} failed for {category}: {e}", flush=True)
            time.sleep(1)

    print(f"⚠️ Skipping category '{category}' after 3 failed attempts.", flush=True)
    return []

# --- Thread Worker ---
def thread_worker(db_config, category, data, target_years):
    predictions = predict_future(category, data["history"], target_years)
    if predictions:
        save_predictions(db_config, category, predictions)

# --- Main Runner ---
def run_prediction_pipeline(db_config):
    print("📊 Fetching historical totals...", flush=True)
    data_by_category = fetch_historical_data(db_config)

    if not data_by_category:
        print("❌ No data found.", flush=True)
        return

    any_category_data = next(iter(data_by_category.values()))
    last_year = any_category_data["last_year"]
    target_years = list(range(last_year + 1, last_year + 6))

    clear_future_table(db_config)

    threads = []
    for category, data in data_by_category.items():
        t = threading.Thread(target=thread_worker, args=(db_config, category, data, target_years))
        t.start()
        threads.append(t)
        time.sleep(0.2)

    for t in threads:
        t.join()

    print("\n✅ All predictions complete and saved.", flush=True)
