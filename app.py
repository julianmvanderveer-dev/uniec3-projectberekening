"""
Uniec3 Projectberekening Samensteller — Flask app met Mollie iDEAL betaling.

Flow:
  GET  /                  → uploadpagina
  POST /upload            → merge + opslaan → JSON {redirect: /checkout/<id>}
  GET  /checkout/<id>     → prijs + klantgegevensformulier
  POST /pay/<id>          → Mollie betaling aanmaken → redirect iDEAL
  GET  /return            → status ophalen → succes / wacht / fout
  GET  /wait/<id>         → wachtpagina (auto-refresh via meta-refresh)
  POST /webhook           → Mollie callback bij statuswijziging
  GET  /success/<id>      → download-knop
  GET  /download/<id>     → .uniec3 bestand sturen
  GET  /download-invoice/<id> → factuur-PDF sturen
  GET  /admin?key=        → betalingsoverzicht
"""

import io
import os
import time
import uuid
import threading
from datetime import datetime

from flask import (
    Flask, render_template, request, send_file,
    jsonify, redirect, url_for, flash,
)
from mollie.api.client import Client as MollieClient
from fpdf import FPDF

from merger import merge_uniec3
from config import Config

# ── App ───────────────────────────────────────────────────────────────────────
app = Flask(__name__)
app.config.from_object(Config)

# ── Mollie ────────────────────────────────────────────────────────────────────
mollie = MollieClient()
mollie.set_api_key(os.environ.get("MOLLIE_API_KEY", "test_xxxx"))

# ── In-memory opslag ─────────────────────────────────────────────────────────
_store: dict          = {}   # {file_id: {...}}
_lock                 = threading.RLock()  # RLock: zelfde thread mag meerdere keren acquiren
_invoice_seq          = [1]  # [0] = volgende factuurnummer


# ── Helpers ───────────────────────────────────────────────────────────────────

def _next_invoice_nr() -> str:
    with _lock:
        nr = _invoice_seq[0]
        _invoice_seq[0] += 1
    return f"UNI-{datetime.utcnow().year}-{nr:04d}"


def _cleanup():
    """Verwijder sessies ouder dan 2 uur."""
    cutoff = time.time() - 7200
    with _lock:
        stale = [k for k, v in _store.items() if v.get("created_at", 0) < cutoff]
        for k in stale:
            del _store[k]


def _generate_invoice_pdf(entry: dict) -> bytes:
    """Genereer een eenvoudige factuur-PDF met fpdf2."""
    pdf = FPDF()
    pdf.add_page()
    pdf.set_margins(20, 20, 20)

    # Koptekst
    pdf.set_font("Helvetica", "B", 18)
    pdf.cell(0, 12, "Factuur", ln=True)
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 6, f"Factuurnummer : {entry['invoice_nr']}", ln=True)
    pdf.cell(0, 6, f"Factuurdatum  : {entry['invoice_date']}", ln=True)
    pdf.ln(6)

    # Klantgegevens
    customer = entry.get("customer", {})
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 6, "Klantgegevens", ln=True)
    pdf.set_draw_color(0, 120, 212)
    pdf.set_line_width(0.4)
    pdf.line(20, pdf.get_y(), 190, pdf.get_y())
    pdf.ln(2)
    pdf.set_font("Helvetica", "", 10)
    for veld in ("naam", "bedrijf", "email"):
        if customer.get(veld):
            pdf.cell(0, 6, customer[veld], ln=True)
    if customer.get("btw_nr"):
        pdf.cell(0, 6, f"BTW-nummer: {customer['btw_nr']}", ln=True)
    pdf.ln(6)

    # Tabelkop
    pdf.set_fill_color(0, 120, 212)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(120, 8, "Omschrijving", fill=True)
    pdf.cell(0,   8, "Bedrag (EUR)", fill=True, align="R", ln=True)
    pdf.set_text_color(0, 0, 0)
    pdf.set_font("Helvetica", "", 10)

    # Regels
    excl = entry["excl"]
    vat  = entry["vat"]
    incl = entry["incl"]
    pdf.cell(120, 7, f"Uniec3 projectberekening ({entry['n_units']} woning(en))")
    pdf.cell(0, 7, f"{excl:>10.2f}", align="R", ln=True)
    pdf.ln(2)
    pdf.set_draw_color(200, 200, 200)
    pdf.line(20, pdf.get_y(), 190, pdf.get_y())
    pdf.ln(2)
    pdf.cell(120, 6, f"Subtotaal excl. BTW")
    pdf.cell(0, 6, f"{excl:>10.2f}", align="R", ln=True)
    pdf.cell(120, 6, f"BTW {Config.BTW_PCT}%")
    pdf.cell(0, 6, f"{vat:>10.2f}", align="R", ln=True)
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(120, 7, "Totaal incl. BTW")
    pdf.cell(0, 7, f"{incl:>10.2f}", align="R", ln=True)
    pdf.ln(8)

    # Betaalreferentie
    if entry.get("payment_id"):
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(120, 120, 120)
        pdf.cell(0, 6, f"iDEAL-referentie: {entry['payment_id']}", ln=True)

    return bytes(pdf.output())


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html", config=Config)


@app.route("/upload", methods=["POST"])
def upload():
    _cleanup()

    files = request.files.getlist("bestanden")
    if len(files) < 1:
        return jsonify({"error": "Voeg minstens 1 bestand toe."}), 400
    for f in files:
        if not f.filename.lower().endswith(".uniec3"):
            return jsonify({"error": f"'{f.filename}' is geen .uniec3 bestand."}), 400

    # Bestandsnamen ophalen vóórdat we de streams inlezen
    filenames = [f.filename for f in files]

    try:
        file_objects = [io.BytesIO(f.read()) for f in files]
        result_bytes, n_units = merge_uniec3(file_objects)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    naam = request.form.get("bestandsnaam", "").strip()
    if not naam:
        naam = "projectberekening_" + datetime.now().strftime("%Y-%m-%d")
    naam = naam.replace(".uniec3", "").rstrip(".")

    excl = Config.PRIJS_EXCL_BTW
    vat  = round(excl * Config.BTW_PCT / 100, 2)
    incl = round(excl + vat, 2)

    file_id = str(uuid.uuid4())
    with _lock:
        _store[file_id] = {
            "created_at":   time.time(),
            "result_bytes": result_bytes,
            "n_units":      n_units,
            "uitvoernaam":  naam,
            "filenames":    filenames,
            "excl":         excl,
            "vat":          vat,
            "incl":         incl,
            "paid":         False,
            "payment_id":   None,
            "customer":     {},
            "invoice_nr":   None,
            "invoice_date": None,
            "invoice_pdf":  None,
        }

    return jsonify({"redirect": url_for("checkout", file_id=file_id)})


@app.route("/checkout/<file_id>")
def checkout(file_id):
    entry = _store.get(file_id)
    if not entry:
        return "Sessie niet gevonden of verlopen.", 404
    return render_template(
        "checkout.html",
        config=Config,
        file_id=file_id,
        filenames=entry["filenames"],
        uitvoernaam=entry["uitvoernaam"],
        excl=entry["excl"],
        vat=entry["vat"],
        incl=entry["incl"],
    )


@app.route("/pay/<file_id>", methods=["POST"])
def pay(file_id):
    entry = _store.get(file_id)
    if not entry:
        return "Sessie niet gevonden of verlopen.", 404

    # Klantgegevens opslaan
    customer = {
        "naam":    request.form.get("naam", "").strip(),
        "bedrijf": request.form.get("bedrijf", "").strip(),
        "email":   request.form.get("email", "").strip(),
        "btw_nr":  request.form.get("btw_nr", "").strip(),
    }
    with _lock:
        entry["customer"] = customer

    # ── Betalings-bypass via promotiecode ─────────────────────────────────────
    valid_codes = {
        c.strip().upper()
        for c in os.environ.get("BYPASS_CODES", Config.BYPASS_CODES).split(",")
        if c.strip()
    }
    promo = request.form.get("promo_code", "").strip().upper()
    if promo and promo in valid_codes:
        _mark_paid(file_id, entry, payment_id="PROMO")
        return redirect(url_for("success", file_id=file_id))
    # ── Normale Mollie-flow ───────────────────────────────────────────────────

    try:
        payment = mollie.payments.create({
            "amount":      {"currency": "EUR", "value": f"{entry['incl']:.2f}"},
            "description": f"Uniec3 projectberekening — {entry['uitvoernaam']}",
            "redirectUrl": url_for("payment_return", file_id=file_id, _external=True),
            "webhookUrl":  url_for("webhook", _external=True),
            "metadata":    {"file_id": file_id},
            "method":      "ideal",
        })
        with _lock:
            entry["payment_id"] = payment.id
    except Exception as e:
        flash(f"Betaling aanmaken mislukt: {e}", "error")
        return redirect(url_for("checkout", file_id=file_id))

    return redirect(payment.checkout_url)


@app.route("/return")
def payment_return():
    file_id = request.args.get("file_id", "")
    entry   = _store.get(file_id)
    if not entry:
        return "Sessie niet gevonden of verlopen.", 404

    payment_id = entry.get("payment_id")
    if not payment_id:
        return redirect(url_for("checkout", file_id=file_id))

    try:
        payment = mollie.payments.get(payment_id)
    except Exception:
        flash("Betaalstatus kon niet worden opgehaald.", "error")
        return redirect(url_for("checkout", file_id=file_id))

    if payment.status == "paid":
        _mark_paid(file_id, entry, payment_id)
        return redirect(url_for("success", file_id=file_id))
    if payment.status in ("pending", "open", "authorized"):
        return redirect(url_for("wait", file_id=file_id))

    flash("Betaling niet geslaagd. Probeer opnieuw.", "error")
    return redirect(url_for("checkout", file_id=file_id))


@app.route("/wait/<file_id>")
def wait(file_id):
    entry = _store.get(file_id)
    if not entry:
        return "Sessie niet gevonden of verlopen.", 404

    # Controleer of betaling inmiddels binnen is
    payment_id = entry.get("payment_id")
    if payment_id:
        try:
            payment = mollie.payments.get(payment_id)
            if payment.status == "paid":
                _mark_paid(file_id, entry, payment_id)
                return redirect(url_for("success", file_id=file_id))
        except Exception:
            pass

    return render_template("wait.html", file_id=file_id)


@app.route("/webhook", methods=["POST"])
def webhook():
    """Mollie stuurt payment_id zodra status wijzigt."""
    payment_id = request.form.get("id", "")
    if not payment_id:
        return "", 200
    try:
        payment = mollie.payments.get(payment_id)
    except Exception:
        return "", 200
    if payment.status != "paid":
        return "", 200

    # Zoek bijbehorende sessie
    with _lock:
        file_id = next(
            (k for k, v in _store.items() if v.get("payment_id") == payment_id),
            None,
        )
    if file_id:
        entry = _store.get(file_id)
        if entry:
            _mark_paid(file_id, entry, payment_id)

    return "", 200   # altijd 200 teruggeven


def _mark_paid(file_id: str, entry: dict, payment_id: str):
    """Markeer sessie als betaald en genereer factuur (eenmalig)."""
    with _lock:
        if entry.get("paid"):
            return   # al verwerkt
        entry["paid"]         = True
        entry["payment_id"]   = payment_id
        entry["invoice_nr"]   = _next_invoice_nr()
        entry["invoice_date"] = datetime.utcnow().strftime("%d-%m-%Y")

    # Factuur buiten lock genereren (CPU-werk)
    try:
        pdf_bytes = _generate_invoice_pdf(entry)
        with _lock:
            entry["invoice_pdf"] = pdf_bytes
    except Exception:
        pass


@app.route("/success/<file_id>")
def success(file_id):
    entry = _store.get(file_id)
    if not entry:
        return "Sessie niet gevonden of verlopen.", 404
    if not entry.get("paid"):
        return redirect(url_for("payment_return", file_id=file_id))
    return render_template(
        "success.html",
        config=Config,
        file_id=file_id,
        filenames=entry["filenames"],
        uitvoernaam=entry["uitvoernaam"],
    )


@app.route("/download/<file_id>")
def download(file_id):
    entry = _store.get(file_id)
    if not entry:
        return "Sessie niet gevonden of verlopen.", 404
    if not entry.get("paid"):
        return "Betaling nog niet bevestigd.", 403
    return send_file(
        io.BytesIO(entry["result_bytes"]),
        as_attachment=True,
        download_name=f"{entry['uitvoernaam']}.uniec3",
        mimetype="application/octet-stream",
    )


@app.route("/download-invoice/<file_id>")
def download_invoice(file_id):
    entry = _store.get(file_id)
    if not entry:
        return "Sessie niet gevonden of verlopen.", 404
    if not entry.get("paid"):
        return "Betaling nog niet bevestigd.", 403
    pdf = entry.get("invoice_pdf")
    if not pdf:
        # Genereer alsnog
        pdf = _generate_invoice_pdf(entry)
        with _lock:
            entry["invoice_pdf"] = pdf
    return send_file(
        io.BytesIO(pdf),
        as_attachment=True,
        download_name=f"factuur_{entry['invoice_nr']}.pdf",
        mimetype="application/pdf",
    )


@app.route("/admin")
def admin():
    key = request.args.get("key", "")
    if key != os.environ.get("ADMIN_KEY", Config.ADMIN_KEY):
        return "Toegang geweigerd.", 403

    with _lock:
        facturen = [
            {
                "nr":        v.get("invoice_nr", "—"),
                "datum":     v.get("invoice_date", "—"),
                "klant":     v.get("customer", {}).get("naam", "—"),
                "email":     v.get("customer", {}).get("email", "—"),
                "bestanden": ", ".join(v.get("filenames", [])),
                "bedrag":    v.get("excl", 0.0),
                "payment_id":v.get("payment_id", ""),
            }
            for v in _store.values()
            if v.get("paid")
        ]

    return render_template("admin.html", config=Config, facturen=facturen)


if __name__ == "__main__":
    app.run(debug=False)
