"""
app.py — Uniec3 Projectberekening Samensteller

Routes:
  GET  /                   Upload pagina
  POST /upload             Bestanden opslaan → redirect /checkout/<id>
  GET  /checkout/<id>      Overzicht + klantgegevens + betaalknop
  POST /pay/<id>           Mollie betaling aanmaken → iDEAL
  GET  /return             Terugkeer na betaling
  GET  /wait/<id>          Wachtpagina
  POST /webhook            Mollie webhook
  GET  /success/<id>       Successpagina + download
  GET  /download/<id>      .uniec3 bestand downloaden
  GET  /download-invoice/<id>  Factuur PDF downloaden
  GET  /admin?key=...      Factuuroverzicht
"""

import io
import os
import time
import uuid
import threading
from datetime import datetime

from flask import (
    Flask, request, redirect, url_for,
    render_template, send_file, flash,
)
from fpdf import FPDF
from mollie.api.client import Client as MollieClient

from merger import merge_uniec3
import config

# ── App ───────────────────────────────────────────────────────────────────────

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'uniec3-projectberekening-secret')

# ── Mollie ────────────────────────────────────────────────────────────────────

mollie = MollieClient()
mollie.set_api_key(os.environ.get('MOLLIE_API_KEY', 'test_VERVANG_MET_JOUW_SLEUTEL'))

# ── Opslag ────────────────────────────────────────────────────────────────────

_store: dict    = {}
_invoices: list = []
_lock = threading.Lock()


# ── Hulpfuncties ──────────────────────────────────────────────────────────────

def _vat(excl: float) -> float:
    return round(excl * config.VAT_RATE, 2)


def _invoice_nr() -> str:
    return 'BRYNT-' + datetime.now().strftime('%Y%m%d-%H%M%S')


def _cleanup() -> None:
    cutoff = time.time() - 7200
    with _lock:
        stale = [k for k, v in _store.items() if v['created_at'] < cutoff]
        for k in stale:
            del _store[k]


def _generate_invoice_pdf(entry: dict) -> bytes:
    c       = entry['customer']
    excl    = config.PRICE_PER_MERGE
    vat     = _vat(excl)
    incl    = round(excl + vat, 2)
    nr      = entry.get('invoice_nr', _invoice_nr())
    datum   = datetime.now().strftime('%d-%m-%Y')
    n_files = len(entry.get('filenames', []))

    pdf = FPDF()
    pdf.add_page()
    pdf.set_margins(20, 20, 20)
    pdf.set_auto_page_break(auto=True, margin=20)

    pdf.set_font('Helvetica', 'B', 18)
    pdf.set_text_color(19, 78, 74)
    pdf.cell(0, 10, config.BEDRIJF_HANDELSNAAM, ln=True)

    pdf.set_font('Helvetica', '', 9)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 5, config.BEDRIJF_NAAM, ln=True)
    if config.BEDRIJF_ADRES:
        pdf.cell(0, 5, f'{config.BEDRIJF_ADRES}, {config.BEDRIJF_POSTCODE} {config.BEDRIJF_PLAATS}', ln=True)
    if config.BEDRIJF_KVK:
        pdf.cell(0, 5, f'KvK: {config.BEDRIJF_KVK}', ln=True)
    if config.BEDRIJF_BTW:
        pdf.cell(0, 5, f'BTW: {config.BEDRIJF_BTW}', ln=True)

    pdf.ln(8)
    pdf.set_draw_color(200, 200, 200)
    pdf.set_line_width(0.3)
    pdf.line(20, pdf.get_y(), 190, pdf.get_y())
    pdf.ln(6)

    pdf.set_font('Helvetica', 'B', 14)
    pdf.set_text_color(30, 30, 30)
    pdf.cell(0, 8, 'FACTUUR', ln=True)
    pdf.ln(2)

    for label, val in [('Factuurnummer:', nr), ('Factuurdatum:', datum)]:
        pdf.set_font('Helvetica', '', 9)
        pdf.set_text_color(80, 80, 80)
        pdf.cell(60, 6, label, ln=False)
        pdf.set_font('Helvetica', 'B', 9)
        pdf.set_text_color(30, 30, 30)
        pdf.cell(0, 6, val, ln=True)

    pdf.ln(6)

    pdf.set_font('Helvetica', 'B', 9)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 6, 'FACTUUR AAN', ln=True)
    pdf.set_font('Helvetica', '', 9)
    pdf.set_text_color(30, 30, 30)
    for regel in [c.get('naam',''), c.get('bedrijf',''), c.get('email','')]:
        if regel:
            pdf.cell(0, 5, regel, ln=True)
    if c.get('btw_nr'):
        pdf.cell(0, 5, f'BTW-nr: {c["btw_nr"]}', ln=True)

    pdf.ln(8)

    pdf.set_fill_color(240, 253, 250)
    pdf.set_font('Helvetica', 'B', 9)
    pdf.set_text_color(19, 78, 74)
    pdf.cell(100, 7, 'Omschrijving', border='B', fill=True)
    pdf.cell(20,  7, 'Aantal',       border='B', fill=True, align='C')
    pdf.cell(30,  7, 'Prijs',        border='B', fill=True, align='R')
    pdf.cell(30,  7, 'Totaal',       border='B', fill=True, align='R', ln=True)

    pdf.set_font('Helvetica', '', 9)
    pdf.set_text_color(30, 30, 30)
    omschr = f'Samenstellen projectberekening ({n_files} bestanden)'
    pdf.cell(100, 7, omschr)
    pdf.cell(20,  7, '1',                           align='C')
    pdf.cell(30,  7, f'\u20ac {excl:.2f}',          align='R')
    pdf.cell(30,  7, f'\u20ac {excl:.2f}',          align='R', ln=True)
    pdf.ln(4)

    def _rij(lbl, bedrag, bold=False):
        pdf.set_font('Helvetica', 'B' if bold else '', 9)
        pdf.cell(150, 6, lbl, align='R')
        pdf.cell(30,  6, f'\u20ac {bedrag:.2f}', align='R', ln=True)

    _rij('Subtotaal (excl. BTW)', excl)
    _rij(f'BTW {int(config.VAT_RATE * 100)}%', vat)
    pdf.set_draw_color(19, 78, 74)
    pdf.line(120, pdf.get_y(), 190, pdf.get_y())
    pdf.ln(1)
    _rij('Totaal (incl. BTW)', incl, bold=True)

    pdf.ln(8)
    if config.BEDRIJF_IBAN:
        pdf.set_font('Helvetica', 'B', 9)
        pdf.set_text_color(80, 80, 80)
        pdf.cell(0, 5, 'Betaalgegevens', ln=True)
        pdf.set_font('Helvetica', '', 9)
        pdf.set_text_color(30, 30, 30)
        pdf.cell(0, 5, f'IBAN: {config.BEDRIJF_IBAN}', ln=True)
        pdf.cell(0, 5, f'T.n.v.: {config.BEDRIJF_NAAM}', ln=True)
        pdf.cell(0, 5, f'Kenmerk: {nr}', ln=True)

    if entry.get('payment_id'):
        pdf.ln(3)
        pdf.set_font('Helvetica', '', 8)
        pdf.set_text_color(150, 150, 150)
        pdf.cell(0, 5, f'Betaald via iDEAL \u00b7 Referentie: {entry["payment_id"]}', ln=True)

    pdf.ln(10)
    pdf.set_font('Helvetica', 'I', 7)
    pdf.set_text_color(160, 160, 160)
    pdf.multi_cell(0, 4,
        'Aan de uitkomst van de samenvoeging kunnen geen rechten worden ontleend. '
        'Brynt.nl is niet verantwoordelijk voor de juistheid van de samengestelde berekening. '
        'Controleer het resultaat altijd zelf.')

    return bytes(pdf.output())


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html', config=config)


@app.route('/upload', methods=['POST'])
def upload():
    _cleanup()
    files = request.files.getlist('bestanden')
    naam  = request.form.get('bestandsnaam', '').strip()

    if len(files) < 1:
        flash('Voeg minstens 1 bestand toe.', 'error')
        return redirect(url_for('index'))
    for f in files:
        if not f.filename.lower().endswith('.uniec3'):
            flash(f'"{f.filename}" is geen .uniec3 bestand.', 'error')
            return redirect(url_for('index'))

    raw_files  = [(f.filename, f.read()) for f in files]
    filenames  = [fn for fn, _ in raw_files]
    datum      = datetime.now().strftime('%Y-%m-%d')
    uitvoernaam = (naam or f'projectberekening_{datum}').rstrip('.uniec3').rstrip('.')

    file_id = str(uuid.uuid4())
    with _lock:
        _store[file_id] = {
            'raw_files':   raw_files,
            'filenames':   filenames,
            'uitvoernaam': uitvoernaam,
            'result_bytes': None,
            'customer':    {},
            'invoice_nr':  None,
            'payment_id':  None,
            'created_at':  time.time(),
        }
    return redirect(url_for('checkout', file_id=file_id))


@app.route('/checkout/<file_id>')
def checkout(file_id):
    with _lock:
        entry = _store.get(file_id)
    if not entry:
        flash('Sessie verlopen. Upload de bestanden opnieuw.', 'error')
        return redirect(url_for('index'))
    excl = config.PRICE_PER_MERGE
    return render_template('checkout.html',
        file_id=file_id,
        filenames=entry['filenames'],
        uitvoernaam=entry['uitvoernaam'],
        excl=excl,
        vat=_vat(excl),
        incl=round(excl + _vat(excl), 2),
        config=config,
    )


@app.route('/pay/<file_id>', methods=['POST'])
def pay(file_id):
    with _lock:
        entry = _store.get(file_id)
    if not entry:
        flash('Sessie verlopen. Upload de bestanden opnieuw.', 'error')
        return redirect(url_for('index'))

    customer = {
        'naam':    request.form.get('naam', '').strip(),
        'bedrijf': request.form.get('bedrijf', '').strip(),
        'email':   request.form.get('email', '').strip(),
        'btw_nr':  request.form.get('btw_nr', '').strip(),
    }
    if not customer['naam'] or not customer['email']:
        flash('Vul je naam en e-mailadres in voor de factuur.', 'error')
        return redirect(url_for('checkout', file_id=file_id))

    invoice_nr = _invoice_nr()
    with _lock:
        if file_id in _store:
            _store[file_id]['customer']   = customer
            _store[file_id]['invoice_nr'] = invoice_nr

    excl = config.PRICE_PER_MERGE
    incl = round(excl + _vat(excl), 2)
    n    = len(entry['filenames'])

    try:
        payment = mollie.payments.create({
            'amount':      {'currency': 'EUR', 'value': f'{incl:.2f}'},
            'description': f'Uniec3 projectberekening — {n} bestanden',
            'redirectUrl': url_for('payment_return', file_id=file_id, _external=True),
            'webhookUrl':  url_for('webhook', _external=True),
            'metadata':    {'file_id': file_id},
            'method':      'ideal',
        })
    except Exception as e:
        flash(f'Fout bij aanmaken betaling: {e}', 'error')
        return redirect(url_for('checkout', file_id=file_id))

    with _lock:
        if file_id in _store:
            _store[file_id]['payment_id'] = payment.id

    return redirect(payment.checkout_url)


@app.route('/return')
def payment_return():
    file_id = request.args.get('file_id', '')
    with _lock:
        entry = _store.get(file_id)
    if not entry or not entry.get('payment_id'):
        flash('Onbekende sessie. Upload de bestanden opnieuw.', 'error')
        return redirect(url_for('index'))

    try:
        status = mollie.payments.get(entry['payment_id']).status
    except Exception as e:
        flash(f'Kon betaalstatus niet ophalen: {e}', 'error')
        return redirect(url_for('index'))

    if status == 'paid':
        return _do_merge_and_redirect(file_id, entry)
    if status in ('pending', 'open', 'authorized'):
        return redirect(url_for('wait', file_id=file_id))

    flash('Betaling niet geslaagd. Probeer het opnieuw.', 'error')
    return redirect(url_for('checkout', file_id=file_id))


@app.route('/wait/<file_id>')
def wait(file_id):
    with _lock:
        entry = _store.get(file_id)
    if not entry:
        flash('Sessie verlopen.', 'error')
        return redirect(url_for('index'))
    try:
        if mollie.payments.get(entry['payment_id']).status == 'paid':
            return _do_merge_and_redirect(file_id, entry)
    except Exception:
        pass
    return render_template('wait.html', file_id=file_id)


@app.route('/webhook', methods=['POST'])
def webhook():
    payment_id = request.form.get('id', '')
    if not payment_id:
        return '', 200
    try:
        payment = mollie.payments.get(payment_id)
        if payment.status != 'paid':
            return '', 200
        with _lock:
            file_id = next(
                (k for k, v in _store.items() if v.get('payment_id') == payment_id), None)
        if file_id:
            with _lock:
                entry = _store.get(file_id)
            if entry and not entry.get('result_bytes'):
                _run_merge(file_id, entry)
    except Exception:
        pass
    return '', 200


@app.route('/success/<file_id>')
def success(file_id):
    with _lock:
        entry = _store.get(file_id)
    if not entry or not entry.get('result_bytes'):
        flash('Sessie verlopen. Neem contact op als je het bestand niet hebt ontvangen.', 'error')
        return redirect(url_for('index'))
    return render_template('success.html',
        file_id=file_id,
        uitvoernaam=entry['uitvoernaam'],
        filenames=entry['filenames'],
        config=config,
    )


@app.route('/download/<file_id>')
def download(file_id):
    with _lock:
        entry = _store.get(file_id)
    if not entry or not entry.get('result_bytes'):
        flash('Download niet meer beschikbaar. Upload de bestanden opnieuw.', 'error')
        return redirect(url_for('index'))
    buf = io.BytesIO(entry['result_bytes'])
    return send_file(buf, mimetype='application/octet-stream',
                     as_attachment=True,
                     download_name=f'{entry["uitvoernaam"]}.uniec3')


@app.route('/download-invoice/<file_id>')
def download_invoice(file_id):
    with _lock:
        entry = _store.get(file_id)
    if not entry:
        flash('Geen factuur beschikbaar.', 'error')
        return redirect(url_for('index'))
    buf = io.BytesIO(_generate_invoice_pdf(entry))
    nr  = entry.get('invoice_nr', 'factuur')
    return send_file(buf, mimetype='application/pdf',
                     as_attachment=True, download_name=f'{nr}.pdf')


@app.route('/admin')
def admin():
    key = request.args.get('key', '')
    if key != os.environ.get('ADMIN_KEY', config.ADMIN_KEY):
        return 'Toegang geweigerd.', 403
    with _lock:
        facturen = list(reversed(_invoices))
    return render_template('admin.html', facturen=facturen, config=config)


# ── Interne hulpfuncties ──────────────────────────────────────────────────────

def _run_merge(file_id: str, entry: dict) -> bool:
    try:
        file_objects = [io.BytesIO(raw) for _, raw in entry['raw_files']]
        result_bytes, _ = merge_uniec3(file_objects)
    except Exception:
        return False

    with _lock:
        if file_id not in _store:
            return False
        _store[file_id]['result_bytes'] = result_bytes
        _invoices.append({
            'nr':         _store[file_id].get('invoice_nr', ''),
            'datum':      datetime.now().strftime('%d-%m-%Y %H:%M'),
            'klant':      entry.get('customer', {}).get('naam', ''),
            'email':      entry.get('customer', {}).get('email', ''),
            'bestanden':  len(entry.get('filenames', [])),
            'bedrag':     config.PRICE_PER_MERGE,
            'payment_id': entry.get('payment_id', ''),
        })
    return True


def _do_merge_and_redirect(file_id: str, entry: dict):
    if not entry.get('result_bytes'):
        if not _run_merge(file_id, entry):
            flash('Fout bij samenvoegen. Probeer opnieuw.', 'error')
            return redirect(url_for('index'))
    return redirect(url_for('success', file_id=file_id))


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
