import concurrent.futures
import mysql.connector
import requests
import time
import random

# Years and newspapers
all_years = list(range(1991, 2025))
newspapers = [
    {"name": "new york times", "search_term": "new york times site:www.nytimes.com"},
    {"name": "publico", "search_term": "publico site:www.publico.pt"},
    {"name": "abc", "search_term": "abc site:www.abc.es"},
    {"name": "the guardian", "search_term": "the guardian site:www.guardian.co.uk"}
]


# DB insert for a single article (fallback)
def insert_article_to_db(conn, article):
    cursor = conn.cursor()
    query = """
    INSERT INTO articles (title, url, snippet, date)
    VALUES (%s, %s, %s, %s)
    """
    values = (
        article.get("title", "No title"),
        article.get("linkToArchive", "No URL"),
        article.get("snippet", "No snippet"),
        int(article.get("date", 0))
    )
    try:
        cursor.execute(query, values)
        conn.commit()
        print(f"Article '{article.get('title')}' inserted successfully.", flush=True)
    except mysql.connector.Error as e:
        print(f"Error inserting article '{article.get('title')}': {e}", flush=True)
        conn.rollback()

# Batch insert
def insert_articles_in_batch(conn, cursor, articles_batch):
    query = """
    INSERT INTO articles (title, url, snippet, date)
    VALUES (%s, %s, %s, %s)
    """
    try:
        cursor.executemany(query, articles_batch)
        conn.commit()
        return True
    except mysql.connector.Error as e:
        print(f"Error during batch insert: {e}", flush=True)
        conn.rollback()
        return False

# Article fetcher for a year + newspaper
def fetch_articles_for_year(search_term, year, newspaper_name, db_config, max_items=500, total_articles=2000, retries=10, timeout=60):
    from_date = f"{year}0101000000"
    to_date = f"{year}1231235959"
    total_pages = total_articles // max_items

    print(f"[{newspaper_name.upper()} {year}] Starting...", flush=True)

    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    for page in range(total_pages):
        offset = page * max_items
        url = (
            f"https://arquivo.pt/textsearch?q={search_term}"
            f"&maxItems={max_items}&offset={offset}&from={from_date}&to={to_date}"
        )

        for attempt in range(retries):
            try:
                response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
                response.raise_for_status()
                data = response.json()
                articles = data.get("response_items", [])

                articles_batch = [
                    (
                        a.get("title", "No title"),
                        a.get("linkToArchive", "No URL"),
                        a.get("snippet", "No snippet"),
                        int(a.get("date", 0))
                    ) for a in articles
                ]

                if not insert_articles_in_batch(conn, cursor, articles_batch):
                    print(f"Retrying batch with smaller size for page {page + 1}.", flush=True)
                    batch_size = len(articles_batch) // 2
                    while batch_size > 0:
                        if insert_articles_in_batch(conn, cursor, articles_batch[:batch_size]):
                            break
                        batch_size //= 2
                print(f"Inserted {len(articles_batch)} articles from page {page + 1}.", flush=True)
                break

            except requests.exceptions.RequestException as e:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"[{newspaper_name.upper()} {year}] Page {page + 1} failed (Attempt {attempt + 1}/{retries}): {e}", flush=True)
                time.sleep(wait_time)

                if attempt == retries - 1:
                    conn.close()
                    return False

        time.sleep(random.uniform(1.0, 2.0))

    conn.close()
    print(f"[{newspaper_name.upper()} {year}] Done and inserted to DB.", flush=True)
    return True

# Threaded year processor for one newspaper
def run_newspaper_threads(newspaper_name, search_term, years, db_config):
    failed_years = []

    def process_year(year):
        success = fetch_articles_for_year(search_term, year, newspaper_name, db_config)
        if not success:
            failed_years.append(year)

    max_workers = 5
    print(f"Launching threads for {newspaper_name.upper()} with max {max_workers} workers...", flush=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(process_year, years)

    if failed_years:
        print(f"\n--- Retrying failed years for {newspaper_name.upper()} ---", flush=True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(process_year, failed_years)

# Chunk helper
def chunked(iterable, n):
    for i in range(0, len(iterable), n):
        yield iterable[i:i + n]

# Master controller
def fetch_and_store_articles(db_config):
    print("Resetting articles table...", flush=True)

    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM articles")
        cursor.execute("ALTER TABLE articles AUTO_INCREMENT = 1")
        conn.commit()
        print("Table cleared and ID reset.", flush=True)
    except mysql.connector.Error as e:
        print(f"Error resetting table: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

    print("Starting article fetching and storing process...", flush=True)

    # Run newspapers in groups of 4
    for batch in chunked(newspapers, 4):
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            for paper in batch:
                executor.submit(
                    run_newspaper_threads,
                    paper["name"],
                    paper["search_term"],
                    all_years,
                    db_config
                )

    print("Article fetching and storing process completed.", flush=True)
