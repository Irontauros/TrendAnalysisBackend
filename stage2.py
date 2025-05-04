import requests
import mysql.connector
import threading
import time
from urllib.parse import urlparse
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Setting up logging configuration
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(message)s")
logger = logging.getLogger()

# Lock to ensure thread-safe print statements
print_lock = threading.Lock()

def thread_safe_print(message):
    with print_lock:
        logger.debug(message)


MODEL = "mistral"
OLLAMA_URL = "http://localhost:11434/api/generate"
CONFIDENCE_THRESHOLD = 60

domain_to_country = {
    "publico.pt": "Portugal",
    "nytimes.com": "USA",
    "washingtonpost.com": "USA",
    "wsj.com": "USA",
    "latimes.com": "USA",
    "expresso.pt": "Portugal",
    "correiomanha.pt": "Portugal",
    "dn.pt": "Portugal",
    "elmundo.es": "Spain",
    "lavanguardia.com": "Spain",
    "abc.es": "Spain",
    "eldiario.es": "Spain",
    "theguardian.com": "UK",
    "bbc.co.uk": "UK",
    "independent.co.uk": "UK",
    "telegraph.co.uk": "UK"
}

allowed_tags = {
    "Politics", "Economy", "Health", "Technology", "Environment", "Science", "Sports"
}

def extract_real_domain(wrapped_url):
    try:
        parts = wrapped_url.split("/http", 1)
        if len(parts) > 1:
            real_url = "http" + parts[1]
            return urlparse(real_url).netloc.lower()
    except Exception:
        pass
    return "unknown"

def get_tags_from_ai(title, snippet):
    prompt = f"""
You are an AI tasked with classifying a news article by assigning relevant tags. Use ONLY from the following list of allowed tags: {', '.join(allowed_tags)}.

Your job:
- For each tag, assess how well it fits the article based on the title and snippet.
- Score each tag from 0 to 100 based on textual relevance (e.g., direct mentions, strong implications).
- Do NOT guess — if a tag is not clearly implied, score it low.
- Keep your explanations short (1 sentence max) and relevant.

Return only this format (and nothing else):
Tag: <tag>, Confidence: <score>, Reason: <short reason>

Example:
Tag: Politics, Confidence: 85, Reason: Mentions election and political parties
Tag: Technology, Confidence: 20, Reason: No reference to tech or innovation

Title: "{title}"
Snippet: "{snippet}"
""".strip()

    try:
        payload = {
            "model": MODEL,
            "prompt": prompt,
            "stream": False
        }
        response = requests.post(OLLAMA_URL, json=payload)
        response.raise_for_status()
        result = response.json()
        return result.get("response", "")
    except Exception as e:
        thread_safe_print("❌ Ollama error: " + str(e))
        return None

def get_tags_with_retry(title, snippet, retries=10, delay=2):
    for attempt in range(retries):
        result = get_tags_from_ai(title, snippet)
        if result:
            return result
        time.sleep(delay)
    return None

def extract_high_confidence_tags(ai_response):
    tags = []
    for line in ai_response.splitlines():
        if "Confidence" in line:
            try:
                # Handle both 'Tag:' and numbered formats
                if "Tag:" in line:
                    tag_part, _ = line.split("Confidence:")
                    raw_tag = tag_part.split("Tag:")[1].strip()
                else:
                    tag_part, _ = line.split("Confidence:")
                    raw_tag = re.split(r'\d+\.\s*', tag_part)[-1].strip()
                
                clean_tag = re.sub(r"[^\w\s]", "", raw_tag).strip()

                # Extract numeric confidence
                score_match = re.search(r"Confidence:\s*(\d+)", line)
                if not score_match:
                    continue
                score = int(score_match.group(1))

                if score >= CONFIDENCE_THRESHOLD and clean_tag in allowed_tags:
                    tags.append(clean_tag)
            except Exception as e:
                thread_safe_print(f"⚠️ Tag parsing error: {e} | Line: {line}")
                continue
    return tags

def clear_totals_table(conn, cursor):
    thread_safe_print("🧹 Clearing totals table...")
    cursor.execute("TRUNCATE TABLE totals;")
    conn.commit()


def update_totals(cursor, year, country, tags):
    for tag in tags:
        cursor.execute(""" 
            INSERT INTO totals (year, country, category, article_count)
            VALUES (%s, %s, %s, 1)
            ON DUPLICATE KEY UPDATE article_count = article_count + 1
        """, (year, country, tag))

def process_article(article):
    article_id, title, url, snippet, timestamp = article

    domain = extract_real_domain(url)
    country = next((v for k, v in domain_to_country.items() if k in domain), "Unknown")

    ai_response = get_tags_with_retry(title, snippet)
    if not ai_response:
        thread_safe_print(f"❌ AI failed for article ID {article_id}")
        return None

    thread_safe_print(f"[AI RAW] Article ID {article_id} Response:\n{ai_response}\n")

    tags = extract_high_confidence_tags(ai_response)
    if not tags:
        thread_safe_print(f"⚠️ No valid tags extracted for article ID {article_id}")

    year = time.gmtime(timestamp).tm_year

    thread_safe_print(f"[ID {article_id}] → Country: {country} | Tags: {tags}")
    return {
        "article_id": article_id,
        "year": year,
        "country": country,
        "tags": tags,
        "title": title,
        "domain": domain
    }

def fetch_and_update_data(db_config, clear_table=False):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        if clear_table:
            clear_totals_table(conn, cursor)

        cursor.execute("SELECT id, title, url, snippet, date FROM articles;")
        data = cursor.fetchall()
        if not data:
            thread_safe_print("⚠️ No articles found in the database.")
            return False

        processed_articles = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(process_article, article) for article in data]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    processed_articles.append(result)
                    thread_safe_print(f"✅ Article ID {result['article_id']} processed.")

        for result in processed_articles:
            update_totals(cursor, result["year"], result["country"], result["tags"])

        conn.commit()
        thread_safe_print(f"✅ Done. Processed {len(processed_articles)} articles. Last article ID: {data[-1][0]}")
        conn.close()
        return True

    except mysql.connector.Error as err:
        thread_safe_print(f"❌ Database error: {err}")
        return False
