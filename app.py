import os
import json
import uuid
import threading
from flask import Flask, render_template, request, jsonify, Response, send_file
from scraper import scrape_full_shop, extract_shop_name, analyze_category, analyze_keywords_auto

app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "etsy-ai-v2-secret")

tasks = {}


def serialize(obj):
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return str(obj)


@app.route("/")
def index():
    env_key = os.environ.get("SCRAPERAPI_KEY", "")
    return render_template("index.html", env_key=env_key)


@app.route("/analyze_shop", methods=["POST"])
def analyze_shop():
    data = request.json
    scraper_key = data.get("scraper_key", "").strip()
    url = data.get("url", "").strip()
    max_listing_pages = int(data.get("max_listing_pages", 3))
    max_review_pages = int(data.get("max_review_pages", 5))

    if not scraper_key:
        return jsonify({"error": "Cle ScraperAPI requise."}), 400
    if not url:
        return jsonify({"error": "URL ou nom de boutique requis."}), 400

    shop_name = extract_shop_name(url)
    if not shop_name:
        return jsonify({"error": "URL invalide."}), 400

    task_id = str(uuid.uuid4())[:8]
    tasks[task_id] = {"status": "running", "progress": 0, "text": "Demarrage...", "result": None, "error": None}

    def run():
        def progress_cb(value, text):
            tasks[task_id]["progress"] = value
            tasks[task_id]["text"] = text

        result, error = scrape_full_shop(
            url, scraper_key,
            progress_callback=progress_cb,
            max_listing_pages=max_listing_pages,
            max_review_pages=max_review_pages,
        )
        if error:
            tasks[task_id]["error"] = error
        else:
            tasks[task_id]["result"] = result
        tasks[task_id]["status"] = "done"

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"task_id": task_id})


@app.route("/analyze_category", methods=["POST"])
def analyze_cat():
    data = request.json
    scraper_key = data.get("scraper_key", "").strip()
    query = data.get("query", "").strip()
    sort = data.get("sort", "most_relevant")
    max_pages = int(data.get("max_pages", 3))
    exclude_digital = data.get("exclude_digital", True)
    max_results = int(data.get("max_results", 0))

    if not scraper_key:
        return jsonify({"error": "Cle ScraperAPI requise."}), 400
    if not query:
        return jsonify({"error": "Mot-cle requis."}), 400

    task_id = str(uuid.uuid4())[:8]
    tasks[task_id] = {"status": "running", "progress": 0, "text": "Demarrage...", "result": None, "error": None, "partial_listings": []}

    def run():
        def progress_cb(value, text, partial=None):
            tasks[task_id]["progress"] = value
            tasks[task_id]["text"] = text
            if partial is not None:
                tasks[task_id]["partial_listings"] = partial

        result, error = analyze_category(
            query, scraper_key,
            max_pages=max_pages,
            sort=sort,
            progress_callback=progress_cb,
            exclude_digital=exclude_digital,
            max_results=max_results,
        )
        if error:
            tasks[task_id]["error"] = error
        else:
            tasks[task_id]["result"] = result
        tasks[task_id]["status"] = "done"

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"task_id": task_id})


@app.route("/analyze_keywords", methods=["POST"])
def analyze_kw():
    data = request.json
    scraper_key = data.get("scraper_key", "").strip()

    if not scraper_key:
        return jsonify({"error": "Cle ScraperAPI requise."}), 400

    task_id = str(uuid.uuid4())[:8]
    tasks[task_id] = {"status": "running", "progress": 0, "text": "Demarrage...", "result": None, "error": None}

    def run():
        def progress_cb(value, text):
            tasks[task_id]["progress"] = value
            tasks[task_id]["text"] = text

        result, error = analyze_keywords_auto(scraper_key, progress_callback=progress_cb)
        if error:
            tasks[task_id]["error"] = error
        else:
            tasks[task_id]["result"] = result
        tasks[task_id]["status"] = "done"

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"task_id": task_id})


@app.route("/progress/<task_id>")
def progress(task_id):
    def generate():
        import time
        sent_count = 0
        while True:
            task = tasks.get(task_id)
            if not task:
                yield f"data: {json.dumps({'status': 'error', 'error': 'Tache introuvable'})}\n\n"
                break
            if task["status"] == "done":
                if task["error"]:
                    yield f"data: {json.dumps({'status': 'error', 'error': task['error']})}\n\n"
                else:
                    yield f"data: {json.dumps({'status': 'done', 'progress': 1.0, 'text': 'Termine !'})}\n\n"
                break

            partial = task.get("partial_listings", [])
            new_items = partial[sent_count:] if len(partial) > sent_count else []
            msg = {
                'status': 'running',
                'progress': task['progress'],
                'text': task['text'],
            }
            if new_items:
                msg['new_listings'] = new_items
                sent_count = len(partial)
            yield f"data: {json.dumps(msg, default=serialize)}\n\n"
            time.sleep(0.5)

    return Response(generate(), mimetype="text/event-stream")


@app.route("/result/<task_id>")
def get_result(task_id):
    task = tasks.get(task_id)
    if not task:
        return jsonify({"error": "Tache introuvable"}), 404
    if task["status"] != "done":
        return jsonify({"error": "Tache en cours"}), 202
    if task["error"]:
        return jsonify({"error": task["error"]}), 500
    return Response(
        json.dumps(task["result"], default=serialize),
        mimetype="application/json"
    )


@app.route("/export_csv", methods=["POST"])
def export_csv():
    import csv
    import io
    data = request.json
    rows = data.get("rows", [])
    filename = data.get("filename", "export.csv")

    si = io.StringIO()
    if rows:
        writer = csv.DictWriter(si, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    output = io.BytesIO()
    output.write(si.getvalue().encode("utf-8"))
    output.seek(0)
    return send_file(output, mimetype="text/csv", as_attachment=True, download_name=filename)


@app.route("/download_project")
def download_project():
    import zipfile
    import io

    zip_buffer = io.BytesIO()
    files_to_include = [
        "app.py",
        "scraper.py",
        "Dockerfile",
        "render_requirements.txt",
        "render.yaml",
    ]
    template_files = [
        ("templates/index.html", "templates/index.html"),
    ]

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for filepath in files_to_include:
            if os.path.exists(filepath):
                zf.write(filepath, filepath)
        for src, dest in template_files:
            if os.path.exists(src):
                zf.write(src, dest)

    zip_buffer.seek(0)
    return send_file(
        zip_buffer,
        mimetype="application/zip",
        as_attachment=True,
        download_name="Etsy-AI-V2.zip",
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
