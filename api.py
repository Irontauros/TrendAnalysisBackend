import os
import mysql.connector
from flask import Flask, jsonify, Response
from flask_cors import CORS
from dotenv import load_dotenv
import subprocess

load_dotenv()

app = Flask(__name__)
CORS(app)

# ------------------ Database Connection ------------------ #
def get_connection():
    db_host = os.environ.get('DB_HOST')

    if os.getenv('GAE_ENV', '').startswith('standard') or os.getenv('K_SERVICE'):
        # For Cloud Run (uses Unix Socket)
        return mysql.connector.connect(
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASS'),
            database=os.environ.get('DB_NAME'),
            unix_socket=f'/cloudsql/{db_host}'  # Cloud SQL connection
        )
    else:
        # For local development (uses public IP)
        return mysql.connector.connect(
            host=db_host,  # Local IP or hostname
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASS'),
            database=os.environ.get('DB_NAME')
        )



# ------------------ Fetch from Totals ------------------ #
def get_totals_data():
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM totals")
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return results
    except mysql.connector.Error as err:
        print(f"[❌ DB Error] {err}")
        return []


# ------------------ Fetch from Future ------------------ #
def get_future_data():
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM future")
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return results
    except mysql.connector.Error as err:
        print(f"[❌ DB Error] {err}")
        return []


# ------------------ Routes ------------------ #
@app.route('/api/data', methods=['GET'])
def show_data():
    data = get_totals_data()
    if data:
        return jsonify(data)
    return jsonify({"message": "No data found"}), 404


@app.route('/api/future', methods=['GET'])
def show_future():
    data = get_future_data()
    if data:
        return jsonify(data)
    return jsonify({"message": "No future data found"}), 404


# ------------------ Run Main Script ------------------ #
@app.route('/run', methods=['POST'])
def run_main_script():
    try:
        print("⚡ Triggering main script...")

        # Start subprocess
        process = subprocess.Popen(
            ["python", "main.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        def generate():
            for line in process.stdout:
                print(line.strip())  # Log output to Docker console
                yield line

            # Ensure we capture stderr
            for err_line in process.stderr:
                print(f"[ERROR] {err_line.strip()}")
                yield err_line

            # 👇 Make sure we wait for the full process to finish
            process.wait()

        return Response(generate(), mimetype='text/plain')

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500


# ------------------ Run Local ------------------ #
if __name__ == '__main__':
    print("🚀 Running Flask server locally in Dev Mode...")
    app.run(host='0.0.0.0', port=8080, debug=True)

