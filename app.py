from flask import Flask, render_template, request, send_file, jsonify
from merger import merge_uniec3
from config import Config
import io
from datetime import datetime

app = Flask(__name__)
app.config.from_object(Config)


@app.route("/")
def index():
    return render_template("index.html", config=Config)


@app.route("/merge", methods=["POST"])
def merge():
    files = request.files.getlist("bestanden")

    if len(files) < 1:
        return jsonify({"error": "Voeg minstens 1 bestand toe."}), 400

    for f in files:
        if not f.filename.lower().endswith(".uniec3"):
            return jsonify({"error": f"'{f.filename}' is geen .uniec3 bestand."}), 400

    try:
        file_objects = [io.BytesIO(f.read()) for f in files]
        result_bytes, n_units = merge_uniec3(file_objects)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    naam = request.form.get("bestandsnaam", "").strip()
    if not naam:
        naam = "projectberekening_" + datetime.now().strftime("%Y-%m-%d")
    naam = naam.replace(".uniec3", "").rstrip(".")
    filename = f"{naam}.uniec3"

    return send_file(
        io.BytesIO(result_bytes),
        as_attachment=True,
        download_name=filename,
        mimetype="application/octet-stream"
    )


if __name__ == "__main__":
    app.run(debug=False)
